package client

import dealPkg "github.com/KeynihAV/exchange/pkg/exchange/deal"

type Client struct {
	ID      int
	Login   string
	TgID    int64
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

type Dialog struct {
	CurrentCommand string
	LastMsg        string
	CurrentOrder   *dealPkg.Order
}
