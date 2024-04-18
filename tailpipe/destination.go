package tailpipe

import (
	"context"
	"errors"
	"sync"

	"github.com/turbot/tailpipe-plugin-sdk/observer"
)

type Destination struct {
	Name string `json:"name"`

	ctx context.Context

	// observers is a list of observers that will be notified of events.
	observers      []observer.ObserverInterface
	observersMutex sync.RWMutex
}

type DestinationOption func(*Destination) error

func NewDestination(ctx context.Context, name string, opts ...DestinationOption) (*Destination, error) {
	d := &Destination{
		Name: name,
	}
	if err := d.Init(ctx, opts...); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *Destination) Init(ctx context.Context, opts ...DestinationOption) error {
	d.ctx = ctx
	for _, opt := range opts {
		if err := opt(d); err != nil {
			return err
		}
	}
	return d.Validate()
}

func (d *Destination) Validate() error {
	if d.Name == "" {
		return errors.New("destination name is required")
	}
	return nil
}

func WithDestinationObservers(observers ...observer.ObserverInterface) DestinationOption {
	return func(d *Destination) error {
		d.observers = append(d.observers, observers...)
		return nil
	}
}

func (d *Destination) AddObserver(o observer.ObserverInterface) {
	d.observersMutex.Lock()
	defer d.observersMutex.Unlock()
	d.observers = append(d.observers, o)
}

func (d *Destination) NotifyObservers(event observer.Event) {
	d.observersMutex.RLock()
	defer d.observersMutex.RUnlock()
	for _, o := range d.observers {
		o.Notify(event)
	}
}

func (d *Destination) Identifier() string {
	return d.Name
}
