//go:build !windows

package plugin

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	goplugin "github.com/hashicorp/go-plugin"
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
	if err := os.WriteFile(os.Getenv(fakePluginPidEnv), []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
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
			"fakeplugin": &shared.TailpipeGRPCPlugin{Impl: &fakePluginImpl{mode: mode}},
		},
		GRPCServer: goplugin.DefaultGRPCServer,
	})
}

// newFakePluginManager returns a manager which runs the fake plugin in the given mode, and the file holding its pid
func newFakePluginManager(t *testing.T, mode string) (*PluginManager, string) {
	t.Helper()
	pidFile := t.TempDir() + "/plugin.pid"
	pm := NewPluginManager()
	pm.buildCommand = func(ctx context.Context, _ *pplugin.Plugin) (*exec.Cmd, error) {
		exe, err := os.Executable()
		if err != nil {
			return nil, err
		}
		// run it through the shell, as a real plugin is
		cmd := shellCommand(ctx, exe)
		cmd.Env = append(os.Environ(), fakePluginModeEnv+"="+mode, fakePluginPidEnv+"="+pidFile)
		return cmd, nil
	}
	return pm, pidFile
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

func TestDescribe(t *testing.T) {
	pm, _ := newFakePluginManager(t, fakeModeDescribe)
	defer pm.Close()

	res, err := pm.Describe(context.Background(), "fakeplugin")
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
	_, err := pm.Describe(ctx, "fakeplugin")
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
	_, err := pm.Describe(ctx, "fakeplugin")
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
	_, err := pm.Describe(ctx, "fakeplugin")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected a cancelled error, got %v", err)
	}
	if _, statErr := os.Stat(pidFile); statErr == nil {
		t.Error("a plugin was started for an already cancelled context")
	}
}

// a plugin which has finished starting is not killed by aborting starts - only by Close
func TestAbortStartsLeavesRunningPluginAlone(t *testing.T) {
	pm, pidFile := newFakePluginManager(t, fakeModeDescribe)
	defer pm.Close()

	if _, err := pm.Describe(context.Background(), "fakeplugin"); err != nil {
		t.Fatalf("Describe returned an error: %v", err)
	}
	pid := pluginPid(t, pidFile)
	pm.abortStarts()
	time.Sleep(300 * time.Millisecond)
	proc, _ := os.FindProcess(pid)
	if err := proc.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("the running plugin was stopped by aborting starts: %v", err)
	}
}
