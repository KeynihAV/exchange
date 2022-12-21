package deal

type Deal struct {
	ID       int64
	BrokerID int32
	ClientID int32
	OrderID  int64
	Ticker   string
	Volume   int32
	Partial  bool
	Time     int32
	Price    float32
	Type     string
}

type Order struct {
	ID              int64
	BrokerID        int32
	ClientID        int32
	Ticker          string
	Volume          int32
	CompletedVolume int32
	Time            int32
	Price           float32
	Type            string
}

type OHLCV struct {
	ID       int64
	Time     int32
	Interval int32
	Open     float32
	High     float32
	Low      float32
	Close    float32
	Volume   int32
	Ticker   string
}
