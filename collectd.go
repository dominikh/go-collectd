package collectd // import "honnef.co/go/collectd"

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

type Conn struct {
	w io.WriteCloser
	r *bufio.Reader
}

// IOError wraps errors that happen while reading or writing. It often
// indicates an error that renders the connection useless.
type IOError struct {
	Err error
}

// Error wraps errors that collectd returns or that occur while
// processing collectd's response.
type Error struct {
	Err error
}

func (e IOError) Error() string {
	return e.Err.Error()
}

func (e Error) Error() string {
	return e.Err.Error()
}

// New creates a collectd connection. Usually you will want to use
// DialUnix instead.
func New(rw io.ReadWriteCloser) *Conn {
	return &Conn{rw, bufio.NewReader(rw)}
}

// DialUnix opens a unix socket and passes it to New.
func DialUnix(name string) (*Conn, error) {
	addr, err := net.ResolveUnixAddr("unix", name)
	if err != nil {
		return nil, IOError{err}
	}
	c, err := net.DialUnix("unix", nil, addr)
	if err != nil {
		return nil, IOError{err}
	}

	return New(c), nil
}

func (c *Conn) readResponse() ([]string, error) {
	var num int
	_, err := fmt.Fscanf(c.r, "%d ", &num)
	if err != nil {
		return nil, IOError{err}
	}
	status, err := c.r.ReadString('\n')
	if err != nil {
		return nil, IOError{err}
	}
	if num < 0 {
		return nil, Error{errors.New(status[:len(status)-1])}
	}

	out := make([]string, num)
	for i := 0; i < num; i++ {
		resp, err := c.r.ReadString('\n')
		if err != nil {
			return out, IOError{err}
		}

		out[i] = resp[:len(resp)-1]
	}

	return out, nil
}

// SendCommand sends an arbitrary command to collectd.
func (c *Conn) SendCommand(command string) ([]string, error) {
	_, err := c.w.Write([]byte(command + "\n"))
	if err != nil {
		return nil, IOError{err}
	}

	return c.readResponse()
}

// GetValue returns the values for an identifier. The map maps names
// to values.
func (c *Conn) GetValue(name string) (map[string]float64, error) {
	res, err := c.SendCommand(fmt.Sprintf(`GETVAL "%s"`, name))
	if err != nil {
		return nil, err
	}

	ret := make(map[string]float64, len(res))
	for _, v := range res {
		fields := strings.SplitN(v, "=", 2)
		f, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			return ret, Error{fmt.Errorf("Could not parse value %q: %s", fields[1], err)}
		}
		ret[fields[0]] = f
	}

	return ret, nil
}

func mapToKV(v map[string]string) string {
	parts := make([]string, 0, len(v))

	for k, v := range v {
		parts = append(parts, fmt.Sprintf(`%s="%s"`, k, v))
	}

	return strings.Join(parts, " ")
}

// PutValue submits values to collectd. Each value can be a number or
// the string "U" to mean undefined. If t is nil, collectd will
// determine the current timestamp. opts is a key=value map of
// options.
func (c *Conn) PutValue(name string, opts map[string]string, t *time.Time, values ...interface{}) error {
	var value []string

	if t != nil {
		value = append(value, fmt.Sprintf("%d", t.Unix()))
	} else {
		value = append(value, "N")
	}

	for _, v := range values {
		value = append(value, fmt.Sprintf("%v", v))
	}

	_, err := c.SendCommand(fmt.Sprintf(`PUTVAL "%s" %s %s`,
		name, mapToKV(opts), strings.Join(value, ":")))

	return err
}

// PutNotif submits a notification to collectd.
func (c *Conn) PutNotif(opts map[string]string, message string) error {
	_, err := c.SendCommand(fmt.Sprintf(`PUTNOTIF %s message="%s"`, mapToKV(opts), message))
	return err
}

// ListValues returns all values known to collectd. The map maps
// identifier to time of last update.
func (c *Conn) ListValues() (map[string]time.Time, error) {
	res, err := c.SendCommand("LISTVAL")
	if err != nil {
		return nil, err
	}
	ret := make(map[string]time.Time, len(res))
	for _, val := range res {
		fields := strings.SplitN(val, " ", 2)
		var sec, msec int64
		n, err := fmt.Sscanf(fields[0], "%d.%d", &sec, &msec)
		if err != nil {
			if n == 1 {
				msec = 0
			} else {
				return nil, Error{fmt.Errorf("Could not parse timestamp %q: %s", fields[0], err)}
			}
		}

		t := time.Unix(sec, msec*1e6)
		ret[fields[1]] = t
	}

	return ret, nil
}

// Flush flushes cached data older than timeout seconds. Use -1 to
// specify no timeout. By specifying plugins and identifiers the
// flushing can be limited to those.
func (c *Conn) Flush(timeout int, plugins []string, identifiers []string) error {
	parts := []string{"FLUSH", "timeout=" + strconv.Itoa(timeout)}
	for _, plugin := range plugins {
		parts = append(parts, fmt.Sprintf(`plugin="%s"`, plugin))
	}
	for _, id := range identifiers {
		parts = append(parts, fmt.Sprintf(`identifier="%s"`, id))
	}
	_, err := c.SendCommand(strings.Join(parts, " "))
	return err
}

// Close closes the underlying io.ReadWriteCloser. If using DialUnix,
// this must be called to properly close the socket. If using New, it
// is optional.
func (c *Conn) Close() error {
	return c.w.Close()
}
