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
			time int NOT NULL,
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
			time int NOT NULL,
			price float8 NOT NULL,
			type varchar(10) NOT NULL,
			shipped int);`)
	if err != nil {
		return nil, err
	}

	return exchangeDB, nil
}

func (ed *ExchangeDB) AddOrder(deal *dealPkg.Order) (int64, error) {

	query := `INSERT INTO orders(brokerID, clientID, ticker, volume, completedVolume, time, price, type)
	values($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id;`
	statement, err := ed.DB.Prepare(query)
	if err != nil {
		return 0, err
	}
	defer statement.Close()

	var lastID int64
	err = statement.QueryRow(deal.BrokerID, deal.ClientID, deal.Ticker, deal.Volume, 0, deal.Time, deal.Price, deal.Type).Scan(&lastID)
	if err != nil {
		return 0, err
	}

	return lastID, nil
}

func (ed *ExchangeDB) DeleteOrder(dealID int64) error {
	_, err := ed.DB.Exec(`DELETE FROM orders WHERE id = $1`, dealID)
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
		Orders.price,
		Orders.completedVolume as completedVolume
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
			&order.Type, &order.Price, &order.CompletedVolume)
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

	partialClose := order.Volume-volumeToClose != 0
	if partialClose {
		result, err = tx.Exec(`UPDATE orders SET completedVolume = $1 WHERE id = $2;`, order.CompletedVolume, order.ID)
	} else {
		result, err = tx.Exec(`DELETE FROM orders WHERE id = $1`, order.ID)
	}
	if err != nil {
		return nil, err
	}
	_, err = result.RowsAffected()
	if err != nil {
		return nil, err
	}

	query := `INSERT INTO deals(orderID, brokerID, clientID, ticker, volume, partial, time, price, type) 
	values($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING id;`
	statement, err := tx.Prepare(query)
	if err != nil {
		return nil, err
	}
	var lastID int64
	err = statement.QueryRow(order.ID, order.BrokerID, order.ClientID, order.Ticker, volumeToClose, partialClose, order.Time, order.Price, order.Type).Scan(&lastID)
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
	result, err := ed.DB.Exec(`UPDATE deals SET shipped = $1 WHERE id = $2`, time.Now().Unix(), dealID)
	if err != nil {
		return err
	}
	_, err = result.RowsAffected()
	if err != nil {
		return err
	}
	return nil
}
