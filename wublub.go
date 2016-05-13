// Package wublub implements a general purpose pubsub system backed by redis.
// Arbitrary messages can be sent to arbitrarily named channels, and routines
// subscribed to those channels can receive those messages. To wublub instances
// can operate in tandem, even from different boxes, as long as they're sharing
// the same redis instance or a redis instance in the same cluster.
package wublub

import (
	"net"
	"sync"
	"time"

	"github.com/mediocregopher/radix.v2/pool"
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
	// Required. A pool of redis connections to use
	*pool.Pool

	// Optional. Timeout to use when reading/writing subscription information to
	// redis. This happens separately from the pool, so a different value than
	// the pool's timeout value is used. Defaults to 5 seconds.
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

	// written to by the readSpin when it sees a subscribe or unsubscribe
	// message, to let Run know that the command went through
	readSubUnsubCh chan struct{}
	readErrCh      chan error

	router map[string]map[chan<- Publish]bool
	rLock  sync.RWMutex
}

// New initializes a Wublub instance and returns it. Run should be calld in
// order for the instance to actually do work.
func New(o Opts) *Wublub {
	if o.Timeout == 0 {
		o.Timeout = 5 * time.Second
	}
	return &Wublub{
		o:              o,
		subscribed:     map[string]bool{},
		subUnsubCmdCh:  make(chan subUnsubCmd),
		readSubUnsubCh: make(chan struct{}),
		router:         map[string]map[chan<- Publish]bool{},
	}
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

// Publish publishes the given message to the channel in the Publish
func (w *Wublub) Publish(p Publish) error {
	return w.o.Cmd("PUBLISH", p.Channel, p.Message).Err
}

func (w *Wublub) getRClient() (*rclient, error) {
	// We only pull a connection out of the pool to get its address. We don't
	// actually use it. This is kind of a hack, but since each conncetion in the
	// pool may have a different address it's unfortunately necessary.
	dummyc, err := w.o.Get()
	if err != nil {
		return nil, err
	}
	network, addr := dummyc.Network, dummyc.Addr
	w.o.Put(dummyc)

	conn, err := net.DialTimeout(network, addr, w.o.Timeout)
	if err != nil {
		return nil, err
	}
	return newRClient(conn, w.o.Timeout), nil
}

func (w *Wublub) confirmCmdRead() error {
	select {
	case <-w.readSubUnsubCh:
	case err := <-w.readErrCh:
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
// publishes of them. This must be called in order to use Wublub. It will take a
// connection from the pool to use and block indefinitely, until an error is hit
// and returned. From there it is at the user's discretion to decide what to do,
// but it is recommended to simply call Run again on an error.
func (w *Wublub) Run() error {
	rc, err := w.getRClient()
	if err != nil {
		return err
	}
	defer rc.c.Close()

	// always reset readErrCh, in case there was a previous Run which had an
	// error which was never read off
	w.readErrCh = make(chan error, 1)
	go w.readSpin(rc)

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
		case err := <-w.readErrCh:
			return err
		}
	}
}

func (w *Wublub) readSpin(rc *rclient) {
	for {
		r := rc.read()
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
			w.readErrCh <- err
			return
		}

		typ, err := arr[0].Str()
		if err != nil {
			w.readErrCh <- err
			return
		} else if typ == "subscribe" || typ == "unsubscribe" {
			w.readSubUnsubCh <- struct{}{}
			continue
		} else if typ != "message" {
			continue
		}

		channel, err := arr[1].Str()
		if err != nil {
			w.readErrCh <- err
			return
		}

		message, err := arr[2].Str()
		if err != nil {
			w.readErrCh <- err
			return
		}

		p := Publish{
			Channel: channel,
			Message: message,
		}

		w.rLock.RLock()
		for ch := range w.router[channel] {
			if w.o.BlockOnPublish {
				ch <- p
			} else {
				select {
				case ch <- p:
				default:
				}
			}
		}
		w.rLock.RUnlock()
	}
}
