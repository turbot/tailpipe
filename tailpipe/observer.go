package tailpipe

import (
	"sync"

	"github.com/turbot/tailpipe-plugin-sdk/observer"
)

type ObserverHandler func(event observer.Event)

type Observer struct {
	Events chan observer.Event
	Handle ObserverHandler
	Active bool
	mutex  sync.RWMutex
}

func NewObserver(handler ObserverHandler) *Observer {
	return &Observer{
		Events: make(chan observer.Event, 10),
		Handle: handler,
		Active: false,
		mutex:  sync.RWMutex{},
	}
}

func (o *Observer) Start() *Observer {
	if o.Active {
		return o
	}
	o.Active = true
	go func() {
		for event := range o.Events {
			o.Handle(event)
		}
	}()
	return o
}

func (o *Observer) Notify(event observer.Event) {
	o.mutex.RLock()
	defer o.mutex.RUnlock()
	if o.Active {
		o.Events <- event
	}
}

func (o *Observer) Stop() {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	if !o.Active {
		return
	}
	o.Active = false
	close(o.Events)
}
