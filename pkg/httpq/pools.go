package httpq

import (
	"bytes"
	"encoding/gob"
	"sync"
)

// codec maintains pools of encoders and decoders which may be used
// concurrently. The purpose of codec is to reduce the work needed to encode and
// decode data continuously.
type codec struct {
	e sync.Pool
	d sync.Pool
}

type encoder struct {
	b *bytes.Buffer
	e *gob.Encoder
}

type decoder struct {
	b *bytes.Buffer
	d *gob.Decoder
}

func newCodec() *codec {
	return &codec{
		e: sync.Pool{
			New: func() interface{} {
				b := new(bytes.Buffer)
				return &encoder{
					b: b,
					e: gob.NewEncoder(b),
				}
			},
		},
		d: sync.Pool{
			New: func() interface{} {
				b := new(bytes.Buffer)
				return &decoder{
					b: b,
					d: gob.NewDecoder(b),
				}
			},
		},
	}
}

func (c *codec) Encode(v interface{}) ([]byte, error) {
	e := c.e.Get().(*encoder)
	defer c.e.Put(e)
	e.b.Reset()
	if err := e.e.Encode(v); err != nil {
		return nil, err
	}
	return e.b.Bytes(), nil
}

func (c *codec) Decode(data []byte, v interface{}) error {
	d := c.d.Get().(*decoder)
	defer c.d.Put(d)
	defer d.b.Reset()
	if _, err := d.b.Write(data); err != nil {
		return err
	}
	return d.d.Decode(v)
}
