//go:build !windows

package plugin

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	goplugin "github.com/hashicorp/go-plugin"
	"github.com/turbot/pipe-fittings/v2/app_specific"
	pplugin "github.com/turbot/pipe-fittings/v2/plugin"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/shared"
)

// these tests run a fake plugin: the test binary runs itself as a plugin process when the env var below is set

const (
	fakePluginModeEnv = "TAILPIPE_TEST_FAKE_PLUGIN_MODE"
	fakePluginPidEnv  = "TAILPIPE_TEST_FAKE_PLUGIN_PID_FILE"

	// the plugin starts and serves, but never answers describe
	fakeModeHangOnDescribe = "hang-on-describe"
	// the plugin answers describe straight away
	fakeModeDescribe = "describe"
	// the plugin never completes the plugin handshake
	fakeModeHangOnStart = "hang-on-start"

	fakePluginName = "fakeplugin"

	// how long the tests wait for a process to go away before failing
	processGoneTimeout = 10 * time.Second
	// the issue asks for a cancel to end the command within 5 seconds
	cancelDeadline = 5 * time.Second
)

func TestMain(m *testing.M) {
	if mode := os.Getenv(fakePluginModeEnv); mode != "" {
		runFakePlugin(mode)
		return
	}
	os.Exit(m.Run())
}

// fakePluginImpl embeds the (nil) server interface, so only Describe is implemented
type fakePluginImpl struct {
	shared.TailpipePluginServer
	mode string
}

func (f *fakePluginImpl) Describe(ctx context.Context, _ *proto.DescribeRequest) (*proto.DescribeResponse, error) {
	if f.mode == fakeModeHangOnDescribe {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &proto.DescribeResponse{}, nil
}

func runFakePlugin(mode string) {
	// record our pid so the test can check we are gone
	pidFile := os.Getenv(fakePluginPidEnv)
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())), 0600); err != nil { //nolint:gosec // a test-owned temp path
		os.Exit(2)
	}
	if mode == fakeModeHangOnStart {
		time.Sleep(time.Minute)
		return
	}
	goplugin.Serve(&goplugin.ServeConfig{
		HandshakeConfig: shared.Handshake,
		Plugins: map[string]goplugin.Plugin{
			// the name matches the alias of the plugin the tests describe
			fakePluginName: &shared.TailpipeGRPCPlugin{Impl: &fakePluginImpl{mode: mode}},
		},
		GRPCServer: goplugin.DefaultGRPCServer,
	})
}

// newFakePluginManager installs the fake plugin (a link to this test binary) into a temporary install dir, where the
// manager finds it as it would a real one, and returns a manager for it and the file holding the plugin's pid
func newFakePluginManager(t *testing.T, mode string) (*PluginManager, string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	pidFile := filepath.Join(t.TempDir(), "plugin.pid")
	t.Setenv(fakePluginModeEnv, mode)
	t.Setenv(fakePluginPidEnv, pidFile)

	oldInstallDir := app_specific.InstallDir
	app_specific.InstallDir = t.TempDir()
	t.Cleanup(func() { app_specific.InstallDir = oldInstallDir })

	pluginFolder := filepath.Join(app_specific.InstallDir, "plugins", pplugin.NewPlugin(fakePluginName).Plugin)
	if err := os.MkdirAll(pluginFolder, 0o755); err != nil { //nolint:gosec // a test-owned temp dir
		t.Fatal(err)
	}
	if err := os.Symlink(exe, filepath.Join(pluginFolder, "fake.plugin")); err != nil {
		t.Fatal(err)
	}
	return NewPluginManager(), pidFile
}

// pluginPid waits for the fake plugin to write its pid
func pluginPid(t *testing.T, pidFile string) int {
	t.Helper()
	deadline := time.Now().Add(processGoneTimeout)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(pidFile); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(b))); err == nil {
				return pid
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the fake plugin never started")
	return 0
}

func requireProcessGone(t *testing.T, pid int) {
	t.Helper()
	proc, err := os.FindProcess(pid)
	if err != nil {
		return
	}
	deadline := time.Now().Add(processGoneTimeout)
	for time.Now().Before(deadline) {
		if err := proc.Signal(syscall.Signal(0)); err != nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("plugin process %d is still running", pid)
}

// describeWithin runs Describe and fails the test if it has not returned within the deadline, rather than hanging
func describeWithin(t *testing.T, pm *PluginManager, ctx context.Context, deadline time.Duration) error { //nolint:revive // t first
	t.Helper()
	errChan := make(chan error, 1)
	go func() {
		_, err := pm.Describe(ctx, fakePluginName)
		errChan <- err
	}()
	select {
	case err := <-errChan:
		return err
	case <-time.After(deadline):
		// stop the plugin so the test does not leave it running
		pm.Close()
		t.Fatalf("Describe did not return within %s of the cancel", deadline)
		return nil
	}
}

func TestDescribe(t *testing.T) {
	pm, _ := newFakePluginManager(t, fakeModeDescribe)
	defer pm.Close()

	res, err := pm.Describe(context.Background(), fakePluginName)
	if err != nil {
		t.Fatalf("Describe returned an error: %v", err)
	}
	if res.PluginName == "" {
		t.Error("expected the plugin name to be set on the response")
	}
}

func TestDescribe_cancelWhileDescribing(t *testing.T) {
	pm, pidFile := newFakePluginManager(t, fakeModeHangOnDescribe)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// cancel once the plugin is up, so we cancel in the describe call
		pluginPid(t, pidFile)
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	begin := time.Now()
	err := describeWithin(t, pm, ctx, cancelDeadline)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected a cancelled error, got %v", err)
	}
	if !strings.Contains(err.Error(), "cancelled") {
		t.Errorf("expected the error to say it was cancelled, got %q", err.Error())
	}
	// what the commands do on return
	pm.Close()
	if elapsed := time.Since(begin); elapsed > cancelDeadline {
		t.Errorf("cancel took %s, want under %s", elapsed, cancelDeadline)
	}
	requireProcessGone(t, pluginPid(t, pidFile))
}

func TestDescribe_cancelWhileStarting(t *testing.T) {
	pm, pidFile := newFakePluginManager(t, fakeModeHangOnStart)
	// the start timeout is a minute by default: the cancel must not wait for it
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		pluginPid(t, pidFile)
		cancel()
	}()

	begin := time.Now()
	err := describeWithin(t, pm, ctx, cancelDeadline)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected a cancelled error, got %v", err)
	}
	pm.Close()
	if elapsed := time.Since(begin); elapsed > cancelDeadline {
		t.Errorf("cancel took %s, want under %s", elapsed, cancelDeadline)
	}
	requireProcessGone(t, pluginPid(t, pidFile))
}

func TestDescribe_alreadyCancelled(t *testing.T) {
	pm, pidFile := newFakePluginManager(t, fakeModeDescribe)
	defer pm.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := pm.Describe(ctx, fakePluginName)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected a cancelled error, got %v", err)
	}
	if _, statErr := os.Stat(pidFile); statErr == nil {
		t.Error("a plugin was started for an already cancelled context")
	}
}
