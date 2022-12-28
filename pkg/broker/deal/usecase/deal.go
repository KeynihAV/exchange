package usecase

import (
	"database/sql"
	"time"

	"github.com/KeynihAV/exchange/pkg/broker/config"
	dealDeliveryPkg "github.com/KeynihAV/exchange/pkg/broker/deal/delivery"
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

func (dm *DealsManager) CreateOrder(order *dealPkg.Order, config *config.Config) (int64, error) {
	order.Time = int32(time.Now().Unix())
	id, err := dm.DR.AddOrder(order)
	if err != nil {
		return 0, err
	}

	exchID, err := dealDeliveryPkg.CreateOrder(order, config)
	if err != nil {
		return 0, err
	}

	err = dm.DR.MarkOrderShipped(id, exchID)
	if err != nil {
		return 0, err
	}

	return id, nil
}

func (dm *DealsManager) NewOrder(orderType string, brokerID int, clientID int32) (*dealPkg.Order, error) {
	return &dealPkg.Order{
		Type:     orderType,
		BrokerID: int32(brokerID),
		ClientID: clientID,
	}, nil
}

func (dm *DealsManager) CancelOrder(id int64, config *config.Config) error {
	exchangeID, err := dm.DR.GetExchangeID(id)
	if err != nil {
		return err
	}

	err = dealDeliveryPkg.CancelOrder(exchangeID, config)
	if err != nil {
		return err
	}

	err = dm.DR.DeleteOrder(id)
	if err != nil {
		return err
	}

	return nil //вообще тут не очень, по идее нужен outbox pattern, чтобы сообщение писалось в бд и доставлялось отдельным потоком до победного
}

func (dm *DealsManager) OrdersByClient(clientID int) ([]*dealPkg.Order, error) {
	return dm.DR.OrdersByClient(clientID)
}

func (dm *DealsManager) DealProcessing(deal *dealPkg.Deal) error {
	//Записать саму сделку
	err := dm.DR.WriteDeal(deal)
	if err != nil {
		return err
	}
	//Удалить\обновить заявку
	//Обновить портфель
	return nil
}
