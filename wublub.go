// Package wublub implements a layer on top of redis subscriptions. Wublub
// clients can subscribe to arbitrary redis channels, whose publishes will be
// written to a channel the client passes in.
package wublub

import (
	"net"
	"sync"
	"time"

	"github.com/mediocregopher/radix.v2/redis"
)

// Publish comprises all the information related to a publish event that a
// subscriber can receive
type Publish struct {
	Message string
	Channel string
}

// Opts are the options which can be passed in when initializing a Wublub
// instance using New.
type Opts struct {
	// Optional. Timeout to use when reading/writing subscription information to
	// redis. Defaults to 5 seconds.
	Timeout time.Duration

	// Optional. If false, when a publish occurs and is attempted to be written
	// to a subscribed channel, and that channel has no readers (and would
	// therefore block), the behavior is to not write to that channel and
	// continue on. If this is true, Wublub will instead block until that
	// channel can be written to.
	BlockOnPublish bool
}

type subUnsubCmd struct {
	unsub    bool
	channels []string
	doneCh   chan struct{}
}

// Wublub is a container for all information needd to publish or subscribe with
// a backing redis instance. All methods on Wublub are thread-safe
type Wublub struct {
	o             Opts
	subscribed    map[string]bool
	subUnsubCmdCh chan subUnsubCmd
	closeCh       chan struct{}

	// set in Run, should only be used within the goroutine calling Run
	rs *readSpinner

	router map[string]map[chan<- Publish]bool
	rLock  sync.RWMutex
}

// New initializes a Wublub instance and returns it. Run should be called in
// order for the instance to actually do work. Opts may be nil
func New(o *Opts) *Wublub {
	if o == nil {
		o = &Opts{}
	}
	if o.Timeout == 0 {
		o.Timeout = 5 * time.Second
	}
	return &Wublub{
		o:             *o,
		subscribed:    map[string]bool{},
		subUnsubCmdCh: make(chan subUnsubCmd),
		closeCh:       make(chan struct{}),
		router:        map[string]map[chan<- Publish]bool{},
	}
}

// Close stops all of Wublub's running go-routines and cleans up all of its
// state. Note that Close will not Empty Wublub's pool.
func (w *Wublub) Close() {
	close(w.closeCh)
}

// Subscribe registers the given chan to receive Publishes from the given
// channels. The given chan should never be closed.
//
// Note that if BlockOnPublish is set to true, it is recommended that you run
// this command in a separate go-routine than the one reading from the given
// channel, otherwise you may end up in a deadlock.
func (w *Wublub) Subscribe(ch chan<- Publish, channels ...string) {
	w.rLock.Lock()
	for _, channel := range channels {
		if w.router[channel] == nil {
			w.router[channel] = map[chan<- Publish]bool{}
		}
		w.router[channel][ch] = true
	}
	w.rLock.Unlock()

	doneCh := make(chan struct{})
	w.subUnsubCmdCh <- subUnsubCmd{channels: channels, doneCh: doneCh}
	<-doneCh
}

// Unsubscribe un-registers the given chan from receiving Publishes from the
// given channels. The given chan must have been used in a Subscribe previously
// for this to have any effect.
//
// Note that if BlockOnPublish is set to true you should not stop reading from
// the given channel until this command has returned.
func (w *Wublub) Unsubscribe(ch chan<- Publish, channels ...string) {
	w.rLock.Lock()
	for _, channel := range channels {
		if w.router[channel] == nil {
			continue
		}
		delete(w.router[channel], ch)
		if len(w.router[channel]) == 0 {
			delete(w.router, channel)
		}
	}
	w.rLock.Unlock()

	doneCh := make(chan struct{})
	w.subUnsubCmdCh <- subUnsubCmd{unsub: true, channels: channels, doneCh: doneCh}
	<-doneCh
}

func (w *Wublub) confirmCmdRead() error {
	select {
	case <-w.rs.subUnsubCh:
	case err := <-w.rs.errCh:
		return err
	}
	return nil
}

func (w *Wublub) doSub(rc *rclient, channels []string, doneCh chan struct{}) error {
	if doneCh != nil {
		defer close(doneCh)
	}
	for _, channel := range channels {
		if w.subscribed[channel] {
			continue
		}
		if err := rc.write("SUBSCRIBE", channel); err != nil {
			return err
		}
		if err := w.confirmCmdRead(); err != nil {
			return err
		}
		w.subscribed[channel] = true
	}
	return nil
}

func (w *Wublub) doUnsub(rc *rclient, channels []string, doneCh chan struct{}) error {
	if doneCh != nil {
		defer close(doneCh)
	}
	for _, channel := range channels {
		if !w.subscribed[channel] {
			continue
		}
		if err := rc.write("UNSUBSCRIBE", channel); err != nil {
			return err
		}
		if err := w.confirmCmdRead(); err != nil {
			return err
		}
		delete(w.subscribed, channel)
	}
	return nil
}

// Run does the actual work of subscribing to redis channels and reading
// publishes of them. This must be called in order to use Wublub. It will create
// a new connection and block until an error is hit and returned. From there it
// is at the user's discretion to decide what to do, but it is recommended to
// simply call Run again on an error.
//
// When an error is hit, all subscribed channels will continue to be subscribed,
// they don't have to do anything. Subsequent calls to Run will pick up where
// the previous ones left off.
//
// Will return nil if Close is called.
func (w *Wublub) Run(network, addr string) error {
	conn, err := net.DialTimeout(network, addr, w.o.Timeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	rc := newRClient(conn, w.o.Timeout)

	runStopCh := make(chan struct{})
	defer close(runStopCh)

	w.rs = &readSpinner{
		rc:         rc,
		errCh:      make(chan error, 1),
		subUnsubCh: make(chan struct{}),
		runStopCh:  runStopCh,
		w:          w,
	}
	go w.rs.spin()

	// It's possible that this isn't the first Run, so we have to re-subscribe
	// to all the previous channels before we can start serving actual requests
	w.rLock.RLock()
	initChannels := make([]string, 0, len(w.router))
	for channel := range w.router {
		initChannels = append(initChannels, channel)
	}
	w.rLock.RUnlock()
	if err = w.doSub(rc, initChannels, nil); err != nil {
		return err
	}

	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			if err = rc.write("PING"); err != nil {
				return err
			}
		case cmd := <-w.subUnsubCmdCh:
			if cmd.unsub {
				err = w.doUnsub(rc, cmd.channels, cmd.doneCh)
			} else {
				err = w.doSub(rc, cmd.channels, cmd.doneCh)
			}
			if err != nil {
				return err
			}
		case err := <-w.rs.errCh:
			return err
		case <-w.closeCh:
			return nil
		}
	}
}

type readSpinner struct {
	rc *rclient
	// the reader will write to errCh if it encounters any errors and
	// immediately exit. Should be buffered by at least 1
	errCh chan error
	// the reader will write to subUnsubCh when it reads off a sub/unsub
	// confirmation message
	subUnsubCh chan struct{}
	// Run will close runStopCh when it's returning and therefore no longer
	// reading/writing these other channels
	runStopCh chan struct{}

	// the wublub instance running this, needed to access the router for pushing
	// publishes to
	w *Wublub
}

func (rs *readSpinner) spin() {
	for {
		r := rs.rc.read()
		if r.Err != nil {
			if redis.IsTimeout(r) {
				continue
			}
			return
		}

		if str, _ := r.Str(); str == "PONG" {
			continue
		}

		arr, err := r.Array()
		if err != nil {
			rs.errCh <- err
			return
		}

		typ, err := arr[0].Str()
		if err != nil {
			rs.errCh <- err
			return
		} else if typ == "subscribe" || typ == "unsubscribe" {
			select {
			case <-rs.runStopCh:
				return
			case rs.subUnsubCh <- struct{}{}:
			}
			continue
		} else if typ != "message" {
			continue
		}

		channel, err := arr[1].Str()
		if err != nil {
			rs.errCh <- err
			return
		}

		message, err := arr[2].Str()
		if err != nil {
			rs.errCh <- err
			return
		}

		p := Publish{
			Channel: channel,
			Message: message,
		}

		// Check if the Run is still going before we do any publishing. This
		// probably isn't a 100% guard against race-conditions, but it's
		// something.
		select {
		case <-rs.runStopCh:
			return
		default:
		}

		rs.w.rLock.RLock()
		for ch := range rs.w.router[channel] {
			if rs.w.o.BlockOnPublish {
				ch <- p
			} else {
				select {
				case ch <- p:
				default:
				}
			}
		}
		rs.w.rLock.RUnlock()
	}
}
