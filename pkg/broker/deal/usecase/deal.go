package usecase

import (
	"database/sql"
	"time"

	dealRepoPkg "github.com/KeynihAV/exchange/pkg/broker/deal/repo"
	dealPkg "github.com/KeynihAV/exchange/pkg/exchange/deal"
)

type DealsManager struct {
	DR *dealRepoPkg.DealRepo
}

func NewDealsManager(db *sql.DB) (*DealsManager, error) {
	dr, err := dealRepoPkg.NewDealRepo(db)
	if err != nil {
		return nil, err
	}

	return &DealsManager{DR: dr}, nil
}

func (dm *DealsManager) CreateOrder(order *dealPkg.Order) (int64, error) {
	order.Time = int32(time.Now().Unix())
	return dm.DR.AddOrder(order)
}

func (dm *DealsManager) NewOrder(orderType string, brokerID int, clientID int32) (*dealPkg.Order, error) {
	return &dealPkg.Order{
		Type:     orderType,
		BrokerID: int32(brokerID),
		ClientID: clientID,
	}, nil
}
