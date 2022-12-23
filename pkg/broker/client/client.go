package client

type Client struct {
	ID      int
	Login   string
	TgID    int
	ChatID  int64
	Balance float32
}

type Position struct {
	ID       int
	ClientID int32
	Ticker   string
	Volume   int32
	Price    float32
	Total    float32
}
