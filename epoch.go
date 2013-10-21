package iproto

import (
	"time"
)

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

func (e Epoch) Sub(o Epoch) time.Duration {
	return time.Duration(e - o)
}

func (e Epoch) Add(dur time.Duration) Epoch {
	return e + Epoch(dur)
}

func (e Epoch) SubTime(tm time.Time) time.Duration {
	return time.Duration(e) - tm.Sub(epoch)
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

func (e Epoch) WillExpire(after time.Duration) bool {
	return e > 0 && e-Epoch(after) < NowEpoch()
}

func (e Epoch) String() string {
	return e.Time().String()
}

func (e Epoch) Remains() time.Duration {
	return time.Duration(e - NowEpoch())
}

func (e Epoch) Elapsed() time.Duration {
	return time.Duration(NowEpoch() - e)
}
