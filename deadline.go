package iproto

import (
	"errors"
	"log"
	"time"
)

// Timeout is a default timeouts for requests sent to service.
type Timeout struct {
	Send    time.Duration
	Receive time.Duration
}

// SetDefaults will return new Timeout with "fixed" Send and Receive values
// Note:
// - if Receive is 0 , than Send also 0
// - Receive automatically adjusted to 1ms, if it is not zero and less than 1ms
// - Send could not be greater than Receive
// - if Send == 0, than Send = (Receive > 10ms) ? Receive - 5ms : Receive / 2
func (tm Timeout) SetDefaults() Timeout {
	if tm.Receive == 0 {
		tm.Send = 0
		return tm
	}
	if tm.Receive < time.Millisecond {
		tm.Receive = time.Millisecond
	}
	if tm.Send > tm.Receive {
		log.Panicf("iproto.Timeout.Send should not be greater than Receive (got %s > %s)", tm.Send, tm.Receive)
	}
	if tm.Send == 0 {
		switch {
		case tm.Receive > 10*time.Millisecond:
			tm.Send = tm.Receive - 5*time.Millisecond
		default:
			tm.Send = tm.Receive / 2
		}
	}
	return tm
}

func (tm Timeout) Zero() bool {
	return tm.Receive <= 0
}

func (tm Timeout) Deadline(now time.Time) (d Deadline) {
	if tm.Receive == 0 {
		return
	}
	tm = tm.SetDefaults()
	eNow := NewEpoch(now)
	d.Send = eNow.Add(tm.Send)
	d.Receive = eNow.Add(tm.Receive)
	return
}

func (tm Timeout) NowDeadline() Deadline {
	return tm.Deadline(time.Now())
}

var DeadlineInPast = errors.New("Deadline is in a past")

// Deadline is a last point in time when request should be performed.
// Deadline.Send is a last time when request could be sent to a socket
// Deadline.Receive is a last time when service will react on receiving response
// When any of Send or Receive time expires, service will call callback with error,
// and request Code will be set to iproto.RC_Timeout
type Deadline struct {
	Send    Epoch
	Receive Epoch
}

func (d Deadline) SendTime() time.Time {
	return d.Send.Time()
}

func (d Deadline) ReceiveTime() time.Time {
	return d.Receive.Time()
}

func (d Deadline) Zero() bool {
	return d.Receive.Zero()
}

func (d Deadline) Check() {
	if d.Send > d.Receive {
		log.Panicf("Send should not be after Recv (%v is after %v)", d.Send, d.Receive)
	}
}

// NewRecvDeadline returns new deadline for given receive timeout
func NewRecvDeadline(recv time.Time) (d Deadline, err error) {
	now := time.Now()
	if recv.Before(now) {
		err = DeadlineInPast
		return
	}
	tm := Timeout{Receive: recv.Sub(now)}
	d = tm.Deadline(now)
	return
}

// NewSendRecvDeadline returns new deadline for given send and recv points in time
func NewSendRecvDeadline(send, recv time.Time) (d Deadline, err error) {
	now := time.Now()
	if recv.Before(now) || send.Before(now) {
		err = DeadlineInPast
		return
	}
	tm := Timeout{Receive: recv.Sub(now), Send: send.Sub(now)}
	d = tm.Deadline(now)
	return
}

type Epoch time.Duration

var epoch = time.Date(2010, time.January, 1, 0, 0, 0, 0, time.UTC)

func NewEpoch(tm time.Time) Epoch {
	return Epoch(tm.Sub(epoch))
}

func NowEpoch() Epoch {
	return Epoch(time.Now().Sub(epoch))
}

func (e Epoch) Before(o Epoch) bool {
	return e < o
}

func (e Epoch) SubTime(tm time.Time) time.Duration {
	return time.Duration(e) - tm.Sub(epoch)
}

func (e Epoch) Sub(o Epoch) time.Duration {
	return time.Duration(e - o)
}

func (e Epoch) Add(dur time.Duration) Epoch {
	return e + Epoch(dur)
}

func (e Epoch) Time() time.Time {
	if e > 0 {
		return epoch.Add(time.Duration(e))
	} else {
		return time.Time{}
	}
}

func (e Epoch) Zero() bool {
	return e == 0
}

func (e Epoch) String() string {
	return e.Time().String()
}
