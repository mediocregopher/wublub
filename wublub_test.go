package wublub

import (
	"log"
	"sync"
	. "testing"
	"time"

	"github.com/levenlabs/golib/testutil"
	"github.com/mediocregopher/radix.v2/pool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testPool *pool.Pool
var testW *Wublub

func init() {
	var err error
	if testPool, err = pool.New("tcp", "127.0.0.1:6379", 5); err != nil {
		panic(err)
	}

	log.Printf("making new wublub")
	testW = New(nil)
	go func() {
		panic(testW.Run("tcp", "127.0.0.1:6379", nil))
	}()
	log.Printf("done initializing")
}

func TestUnderLoad(t *T) {
	stopCh := make(chan bool)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		gottemCh := make(chan bool)
		readCh := make(chan Publish, 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				p := Publish{
					Channel: testutil.RandStr(),
					Message: testutil.RandStr(),
				}
				testW.Subscribe(readCh, p.Channel)
				require.Nil(t, testPool.Cmd("PUBLISH", p.Channel, p.Message).Err)
				p2 := <-readCh
				assert.Equal(t, p, p2)
				testW.Unsubscribe(readCh, p.Channel)

				select {
				case gottemCh <- true:
				case <-stopCh:
					return
				}
			}
		}()
		go func() {
			for {
				select {
				case <-gottemCh:
				case <-time.After(1 * time.Second):
					t.Fatal("took longer than one second to process a publish")
				case <-stopCh:
					return
				}
			}
		}()
	}

	log.Printf("running for 10 seconds")
	time.Sleep(10 * time.Second)
	log.Printf("stopping test")
	close(stopCh)
	wg.Wait()

	testW.rLock.RLock()
	assert.Empty(t, testW.subscribed)
	assert.Empty(t, testW.router)
	testW.rLock.RUnlock()
}
