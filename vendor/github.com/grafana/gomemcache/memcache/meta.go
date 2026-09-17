package memcache

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
)

// Sighting classifies the outcome of a MetaGet call.
type Sighting int

const (
	// FirstMiss is the zero value (the safe default if a MetaGetResult is
	// ever left unset): the key was missing, and either nobody has asked for
	// it recently (a fresh vivify stub was just created, the "W" meta flag)
	// or vivifyTTL was 0 so no vivify/sighting-tracking happened at all.
	// Don't cache yet - this might be a one-hit-wonder.
	FirstMiss Sighting = iota

	// SubsequentMiss means the key was missing, but a vivify stub already
	// existed from an earlier sighting (the "Z" meta flag): someone already
	// asked for this key recently. Worth caching now.
	SubsequentMiss

	// Found means the key had real data cached already; Value holds it.
	Found
)

// MetaGetResult is the result of a MetaGet call.
type MetaGetResult struct {
	// Sighting classifies the result; see Sighting's values.
	Sighting Sighting

	// Value is the item's value, if Sighting == Found.
	Value []byte

	// Flags are the item's client flags, if Sighting == Found.
	Flags uint32

	// TTLRemaining is the item's remaining TTL in seconds, or -1 if it has none.
	TTLRemaining int32
}

// MetaGet issues a memcached meta get ("mg") for key, requesting the value,
// flags, and remaining TTL. If vivifyTTL is > 0, a missing key is atomically
// vivified with a stub that expires after vivifyTTL seconds; the first caller
// to do so gets Sighting=FirstMiss, and any caller that arrives while the
// stub is still alive gets Sighting=SubsequentMiss instead. Pass vivifyTTL of
// 0 to skip vivification entirely (a miss just returns Sighting=FirstMiss).
func (c *Client) MetaGet(key string, vivifyTTL int32) (*MetaGetResult, error) {
	if !legalKey(key) {
		return nil, ErrMalformedKey
	}
	var res *MetaGetResult
	err := c.withKeyRw(key, func(cn *conn) error {
		rw := cn.rw
		cmd := "mg " + key + " v f t"
		if vivifyTTL > 0 {
			cmd += " N" + strconv.Itoa(int(vivifyTTL))
		}
		if _, err := fmt.Fprintf(rw, "%s\r\n", cmd); err != nil {
			return err
		}
		if err := rw.Flush(); err != nil {
			return err
		}
		r, err := parseMetaGetResponse(rw.Reader)
		if err != nil {
			return err
		}
		res = r
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func parseMetaGetResponse(r *bufio.Reader) (*MetaGetResult, error) {
	line, err := r.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	line = bytes.TrimSuffix(line, crlf)

	switch {
	case bytes.Equal(line, []byte("EN")):
		return &MetaGetResult{Sighting: FirstMiss, TTLRemaining: -1}, nil
	case bytes.HasPrefix(line, []byte("VA ")):
		return parseMetaVA(line, r)
	case bytes.HasPrefix(line, resultClientErrorPrefix), bytes.HasPrefix(line, resultServerErrorPrefix):
		if err := serverErrorFromLine(append(line, '\r', '\n')); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("memcache: client error from mg: %q", string(line))
	default:
		return nil, fmt.Errorf("memcache: unexpected response line from mg: %q", string(line))
	}
}

func parseMetaVA(line []byte, r *bufio.Reader) (*MetaGetResult, error) {
	fields := bytes.Fields(line) // [0]="VA" [1]=datalen [2:]=flags
	if len(fields) < 2 {
		return nil, fmt.Errorf("memcache: malformed VA line: %q", string(line))
	}
	datalen, err := strconv.Atoi(string(fields[1]))
	if err != nil {
		return nil, fmt.Errorf("memcache: malformed VA datalen: %q", string(line))
	}

	res := &MetaGetResult{Sighting: Found, TTLRemaining: -1}
	for _, f := range fields[2:] {
		if len(f) == 0 {
			continue
		}
		switch f[0] {
		case 'f':
			v, _ := strconv.ParseUint(string(f[1:]), 10, 32)
			res.Flags = uint32(v)
		case 't':
			v, _ := strconv.ParseInt(string(f[1:]), 10, 32)
			res.TTLRemaining = int32(v)
		case 'W':
			res.Sighting = FirstMiss // a fresh vivify means we created a stub, not real data
		case 'Z':
			res.Sighting = SubsequentMiss // a stub with no real data behind it yet
		}
	}

	buf := make([]byte, datalen+2) // +2 for trailing \r\n
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	res.Value = buf[:datalen]
	return res, nil
}

// MetaSet issues a memcached meta set ("ms") for key with the given value,
// client flags, and TTL (0 means no expiration). It always performs an
// unconditional set (equivalent to the classic "set" verb).
func (c *Client) MetaSet(key string, value []byte, flags uint32, ttl int32) error {
	if !legalKey(key) {
		return ErrMalformedKey
	}
	return c.withKeyRw(key, func(cn *conn) error {
		rw := cn.rw
		if _, err := fmt.Fprintf(rw, "ms %s %d T%d F%d\r\n", key, len(value), ttl, flags); err != nil {
			return err
		}
		if _, err := rw.Write(value); err != nil {
			return err
		}
		if _, err := rw.Write(crlf); err != nil {
			return err
		}
		if err := rw.Flush(); err != nil {
			return err
		}
		line, err := rw.ReadSlice('\n')
		if err != nil {
			return err
		}
		switch {
		case bytes.Equal(line, []byte("HD\r\n")):
			return nil
		case bytes.Equal(line, []byte("NS\r\n")):
			return ErrNotStored
		default:
			if err := serverErrorFromLine(line); err != nil {
				return err
			}
			return fmt.Errorf("memcache: unexpected response line from ms: %q", string(line))
		}
	})
}
