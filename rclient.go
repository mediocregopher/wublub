package wublub

import (
	"net"
	"time"

	"github.com/mediocregopher/radix.v2/redis"
)

// Wherein I write a little custom redis client that can read and write
// separately

type rclient struct {
	t time.Duration
	c net.Conn
	r *redis.RespReader
}

func newRClient(c net.Conn, t time.Duration) *rclient {
	return &rclient{
		t: t,
		c: c,
		r: redis.NewRespReader(c),
	}
}

func (rc *rclient) write(c ...interface{}) error {
	rc.c.SetWriteDeadline(time.Now().Add(rc.t))
	r := redis.NewRespFlattenedStrings(c)
	_, err := r.WriteTo(rc.c)
	return err
}

func (rc *rclient) read() *redis.Resp {
	rc.c.SetReadDeadline(time.Now().Add(rc.t))
	return rc.r.Read()
}
