package file_watcher

import (
	"github.com/fsnotify/fsnotify"
	filehelpers "github.com/turbot/go-kit/files"
	"github.com/turbot/go-kit/filewatcher"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/tailpipe/internal/constants"
	"log"
	"log/slog"
)

type SourceFileWatcher struct {
	fileWatcherErrorHandler func(error)
	watcher                 *filewatcher.FileWatcher
	handler                 func([]fsnotify.Event)
}

func NewSourceFileWatcher(sourceDir string, handler func(events []fsnotify.Event)) (*SourceFileWatcher, error) {
	w := &SourceFileWatcher{handler: handler}

	watcherOptions := &filewatcher.WatcherOptions{
		Directories: []string{sourceDir},
		Include:     filehelpers.InclusionsFromExtensions(constants.SourceFileExtensions),
		ListFlag:    filehelpers.FilesRecursive,
		EventMask:   fsnotify.Create,
		OnChange: func(events []fsnotify.Event) {
			w.handleFileWatcherEvent(events)
		},
	}
	watcher, err := filewatcher.NewWatcher(watcherOptions)
	if err != nil {
		return nil, err
	}
	w.watcher = watcher

	// set the file watcher error handler, which will get called when there are parsing errors
	// after a file watcher event
	w.fileWatcherErrorHandler = func(err error) {
		log.Printf("[WARN] failed to reload connection config: %s", err.Error())
	}

	watcher.Start()

	slog.Info("created SourceFileWatcher")
	return w, nil
}

func (w *SourceFileWatcher) handleFileWatcherEvent(events []fsnotify.Event) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] SourceFileWatcher caught a panic: %s", helpers.ToError(r).Error())
		}
	}()
	slog.Debug("SourceFileWatcher handleFileWatcherEvent")
	w.handler(events)
	slog.Debug("SourceFileWatcher handleFileWatcherEvent done")
}

func (w *SourceFileWatcher) Close() {
	w.watcher.Close()
}
