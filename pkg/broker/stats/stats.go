package stats

import "time"

type OHLCV struct {
	Time     time.Time
	TimeInt  int32
	Interval int32
	Open     float32
	High     float32
	Low      float32
	Close    float32
	Volume   int32
	Ticker   string
}
