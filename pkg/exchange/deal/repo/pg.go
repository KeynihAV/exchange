package repo

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	configPkg "github.com/KeynihAV/exchange/pkg/exchange/config"
	dealPkg "github.com/KeynihAV/exchange/pkg/exchange/deal"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type ExchangeDB struct {
	DB *sql.DB
}

func NewExchangeDB(db *sql.DB, config *configPkg.ExchangeConfig) (*ExchangeDB, error) {
	db, err := initDB(db, config)
	if err != nil {
		return nil, err
	}

	return &ExchangeDB{
		DB: db,
	}, nil
}

func initDB(db *sql.DB, config *configPkg.ExchangeConfig) (*sql.DB, error) {
	dbName := "exchange"

	var DBMS *sql.DB
	var err error
	if db == nil {
		DBMS, err = sql.Open("pgx", config.PGConnString)
		if err != nil {
			return nil, err
		}
	} else {
		DBMS = db
	}

	err = DBMS.Ping()
	if err != nil {
		return nil, err
	}

	rows, err := DBMS.Query(`SELECT 1 FROM pg_database WHERE datname = $1`, dbName)
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		_, err = DBMS.Exec(fmt.Sprintf(`CREATE DATABASE %v`, dbName))
		if err != nil {
			return nil, err
		}
	}

	var exchangeDB *sql.DB
	if db == nil {
		err = DBMS.Close()
		if err != nil {
			return nil, err
		}

		exchangeDB, err = sql.Open("pgx", fmt.Sprintf("%v dbname=%v", config.PGConnString, dbName))
		if err != nil {
			return nil, err
		}
	} else {
		exchangeDB = db
	}

	err = exchangeDB.Ping()
	if err != nil {
		return nil, err
	}

	_, err = exchangeDB.Exec(
		`CREATE TABLE IF NOT EXISTS orders(
			id SERIAL PRIMARY KEY,
			brokerID int NOT NULL,
			clientID int NOT NULL,
			ticker varchar(200) NOT NULL,
			volume int NOT NULL,
			completedVolume int NOT NULL,
			time time NOT NULL,
			price float8 NOT NULL,
			type varchar(10) NOT NULL);
		CREATE UNIQUE INDEX IF NOT EXISTS sell_idx ON orders (ticker, type, price, time, id);`)
	if err != nil {
		return nil, err
	}

	_, err = exchangeDB.Exec(
		`CREATE TABLE IF NOT EXISTS deals(
			id SERIAL PRIMARY KEY,
			orderID int NOT NULL,
			brokerID int NOT NULL,
			clientID int NOT NULL,
			ticker varchar(200) NOT NULL,
			volume int NOT NULL,
			partial boolean NOT NULL,
			time time NOT NULL,
			price float8 NOT NULL,
			type varchar(10) NOT NULL,
			shipped time NOT NULL);`)
	if err != nil {
		return nil, err
	}

	return exchangeDB, nil
}

func (ed *ExchangeDB) AddOrder(deal *dealPkg.Order) (int64, error) {
	result, err := ed.DB.Exec(`INSERT INTO orders(brokerID, clientID, ticker, volume, completedVolume, time, price, type)
		values($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		deal.BrokerID, deal.ClientID, deal.Ticker, deal.Volume, 0, deal.Time, deal.Price, deal.Type)
	if err != nil {
		return 0, err
	}

	lastID, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}

	return lastID, nil
}

func (ed *ExchangeDB) DeleteOrder(dealID int64) error {
	_, err := ed.DB.Exec(`DELETE FROM orders WHERE id = ?`, dealID)
	if err != nil {
		return err
	}

	return nil
}

func (ed *ExchangeDB) GetOrdersForClose(ticker string, price float32) ([]*dealPkg.Order, error) {
	queryResult, err := ed.DB.Query(`
	SELECT 
		Orders.id,
		Orders.brokerid,
		Orders.clientid,
		Orders.ticker,
		Orders.volume -	Orders.completedVolume as volume,
		Orders.time,
		Orders.type,
		Orders.price
	FROM orders as Orders
	WHERE Orders.ticker = $1 AND Orders.price = $2
	ORDER BY 
		Orders.time, Orders.id`, ticker, price)
	if err != nil {
		return nil, err
	}
	defer queryResult.Close()

	result := make([]*dealPkg.Order, 0)
	for queryResult.Next() {
		order := &dealPkg.Order{}
		err = queryResult.Scan(&order.ID, &order.BrokerID, &order.ClientID, &order.Ticker, &order.Volume, &order.Time,
			&order.Type, &order.Price)
		if err != nil {
			return nil, err
		}
		result = append(result, order)
	}

	return result, nil
}

func (ed *ExchangeDB) MakeDeal(order *dealPkg.Order, volumeToClose int32) (*dealPkg.Deal, error) {
	var result sql.Result
	var err error

	tx, err := ed.DB.BeginTx(context.TODO(), &sql.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	partialClose := order.Volume != volumeToClose
	if partialClose {
		result, err = ed.DB.Exec(`UPDATE orders SET completedVolume = ? WHERE id = ?;`, order.CompletedVolume, order.ID)
	} else {
		result, err = ed.DB.Exec(`DELETE FROM orders WHERE id = ?`, order.ID)
	}
	if err != nil {
		return nil, err
	}
	_, err = result.RowsAffected()
	if err != nil {
		return nil, err
	}

	//тут нужно сделать вставку в сделки и похоже нужно создать отдельные объекты заказ и сделка
	resultDeal, err := ed.DB.Exec(`INSERT INTO deals(orderID, brokerID, clientID, ticker, volume, partial, time, price, type)
		values($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		order.ID, order.BrokerID, order.ClientID, order.Ticker, volumeToClose, partialClose, order.Time, order.Price)
	if err != nil {
		return nil, err
	}
	lastID, err := resultDeal.LastInsertId()
	if err != nil {
		return nil, err
	}

	newDeal := &dealPkg.Deal{
		ID:       lastID,
		BrokerID: order.BrokerID,
		ClientID: order.ClientID,
		OrderID:  order.ID,
		Ticker:   order.Ticker,
		Volume:   volumeToClose,
		Partial:  partialClose,
		Time:     int32(time.Now().Unix()),
		Price:    order.Price,
		Type:     order.Type,
	}
	return newDeal, nil
}

func (ed *ExchangeDB) MarkDealShipped(dealID int64) error {
	result, err := ed.DB.Exec(`UPDATE deals SET shipped = ? WHERE id = ?`, time.Now(), dealID)
	if err != nil {
		return err
	}
	_, err = result.RowsAffected()
	if err != nil {
		return err
	}
	return nil
}
