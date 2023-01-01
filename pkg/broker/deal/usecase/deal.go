package usecase

import (
	"context"
	"database/sql"
	"time"

	dealDeliveryPkg "github.com/KeynihAV/exchange/pkg/broker/deal/delivery"
	dealRepoPkg "github.com/KeynihAV/exchange/pkg/broker/deal/repo"
	"github.com/KeynihAV/exchange/pkg/config"
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

	tx, err := dm.DR.DB.BeginTx(context.TODO(), &sql.TxOptions{})
	if err != nil {
		return err
	}

	err = dm.DR.DeleteOrder(id, tx)
	if err != nil {
		tx.Rollback()
		return err
	} else {
		tx.Commit()
	}

	return nil //вообще тут не очень, по идее нужен outbox pattern, чтобы сообщение писалось в бд и доставлялось отдельным потоком до победного
}

func (dm *DealsManager) OrdersByClient(clientID int) ([]*dealPkg.Order, error) {
	return dm.DR.OrdersByClient(clientID)
}

func (dm *DealsManager) DealProcessing(deal *dealPkg.Deal) error {
	tx, err := dm.DR.DB.BeginTx(context.TODO(), &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	//Записать саму сделку
	err = dm.DR.WriteDeal(deal, tx)
	if err != nil {
		return err
	}

	//ид заявки по сделке
	orderID, err := dm.DR.GetOrderID(deal.OrderID)
	if err != nil {
		return err
	}

	//Удалить\обновить заявку
	if deal.Partial {
		closedVolume, err := dm.DR.OrderClosedVolume(deal.OrderID, tx)
		if err != nil {
			return err
		}
		dm.DR.UpdateOrderClosedVolume(orderID, closedVolume, tx)
	} else {
		dm.DR.DeleteOrder(orderID, tx)
	}

	//Обновить портфель
	err = dm.DR.UpdatePositionsByClientAndTicker(deal.ClientID, deal.Ticker, tx)
	if err != nil {
		return err
	}

	return nil
}
