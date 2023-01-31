package repo

import (
	"database/sql"

	dealPkg "github.com/KeynihAV/exchange/pkg/exchange/deal"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type DealRepo struct {
	DB *sql.DB
}

func NewDealRepo(db *sql.DB) (*DealRepo, error) {
	_, err := db.Exec(
		`CREATE TABLE IF NOT EXISTS orders(
			id SERIAL PRIMARY KEY,
			exchangeID int,
			brokerID int NOT NULL,
			clientID int NOT NULL,
			ticker varchar(200) NOT NULL,
			volume int NOT NULL,
			completedVolume int NOT NULL,
			time int NOT NULL,
			price float8 NOT NULL,
			type varchar(10) NOT NULL);
		CREATE UNIQUE INDEX IF NOT EXISTS exchangeID_idx ON orders (exchangeID);
		CREATE INDEX IF NOT EXISTS clientID_idx ON orders (clientID);`)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(
		`CREATE TABLE IF NOT EXISTS deals(
			id SERIAL PRIMARY KEY,
			exchangeID int,
			clientID int NOT NULL,
			ticker varchar(200) NOT NULL,
			volume int NOT NULL,
			partial boolean NOT NULL,
			time int NOT NULL,
			price float8 NOT NULL,
			type varchar(10) NOT NULL,
			exchangeOrderID int NOT NULL);
		CREATE INDEX IF NOT EXISTS exchangeID_idx ON deals (exchangeID);
		CREATE INDEX IF NOT EXISTS aggregate_idx ON deals (clientID, ticker);`)
	if err != nil {
		return nil, err
	}

	return &DealRepo{
		DB: db,
	}, nil
}

func (dr *DealRepo) AddOrder(order *dealPkg.Order) (int64, error) {
	query := `INSERT INTO orders(brokerID, clientID, ticker, volume, completedVolume, time, price, type)
	values($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id;`

	statement, err := dr.DB.Prepare(query)
	if err != nil {
		return 0, err
	}
	defer statement.Close()

	var lastID int64
	err = statement.QueryRow(order.BrokerID, order.ClientID, order.Ticker, order.Volume, 0, order.Time, order.Price, order.Type).Scan(&lastID)
	if err != nil {
		return 0, err
	}

	return lastID, nil
}

func (dr *DealRepo) DeleteOrder(id int64, tx *sql.Tx) error {
	result, err := tx.Exec(`DELETE FROM orders WHERE id = $1`, id)
	if err != nil {
		return err
	}
	_, err = result.RowsAffected()
	if err != nil {
		return err
	}
	return nil
}

func (dr *DealRepo) OrdersByClient(clientID int) ([]*dealPkg.Order, error) {
	result, err := dr.DB.Query(`SELECT id, brokerID, clientID, ticker, volume, completedVolume, time, price, type
		 FROM orders WHERE clientID = $1`, clientID)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	orders := make([]*dealPkg.Order, 0)
	for result.Next() {
		order := &dealPkg.Order{}
		err = result.Scan(&order.ID, &order.BrokerID, &order.ClientID, &order.Ticker, &order.Volume, &order.CompletedVolume,
			&order.Time, &order.Price, &order.Type)
		if err != nil {
			return nil, err
		}
		orders = append(orders, order)
	}

	return orders, nil
}

func (dr *DealRepo) GetExchangeID(orderID int64) (int64, error) {
	qr := dr.DB.QueryRow(`SELECT exchangeID
		FROM orders WHERE id = $1`, orderID)

	var exchangeID int64
	err := qr.Scan(&exchangeID)
	if err != nil {
		return 0, err
	}

	return exchangeID, nil
}

func (dr *DealRepo) GetOrderID(exchangeID int64) (int64, error) {
	qr := dr.DB.QueryRow(`SELECT id
		FROM orders WHERE exchangeID = $1`, exchangeID)

	var orderID int64
	err := qr.Scan(&orderID)
	if err != nil {
		return 0, err
	}

	return orderID, nil
}

func (dr *DealRepo) MarkOrderShipped(id, exchangeID int64) error {
	result, err := dr.DB.Exec(`UPDATE orders SET exchangeID = $1 WHERE id = $2`, exchangeID, id)
	if err != nil {
		return err
	}
	_, err = result.RowsAffected()
	if err != nil {
		return err
	}
	return nil
}

func (dr *DealRepo) WriteDeal(deal *dealPkg.Deal, tx *sql.Tx) error {
	result, err := tx.Exec(`INSERT INTO deals(exchangeID, clientID, ticker, volume, partial, time, price, type, exchangeOrderID)
	values($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		deal.ID, deal.ClientID, deal.Ticker, deal.Volume, deal.Partial, deal.Time, deal.Price, deal.Type, deal.OrderID)
	if err != nil {
		return err
	}
	_, err = result.RowsAffected()
	if err != nil {
		return err
	}
	return nil
}

func (dr *DealRepo) UpdateOrderClosedVolume(orderID int64, completedVolume int32, tx *sql.Tx) error {
	result, err := tx.Exec(`UPDATE orders SET completedVolume = completedVolume + $1 WHERE id = $2`,
		completedVolume, orderID)
	if err != nil {
		return err
	}
	_, err = result.RowsAffected()
	if err != nil {
		return err
	}
	return nil
}

func (dr *DealRepo) OrderClosedVolume(exchangeOrderID int64, tx *sql.Tx) (int32, error) {
	qr := tx.QueryRow(`SELECT SUM(volume)
		FROM deals WHERE exchangeOrderID = $1`, exchangeOrderID)

	var closedVolume int32
	err := qr.Scan(&closedVolume)
	if err != nil {
		return 0, err
	}

	return closedVolume, nil
}

func (dr *DealRepo) UpdatePositionsByClientAndTicker(clientID int32, ticker string, tx *sql.Tx) error {
	result, err := tx.Exec(`
		INSERT INTO positions (clientID, ticker, volume, total, price)
		SELECT 
			deals.clientID,
			deals.ticker,
			SUM(CASE WHEN deals.type = $1 THEN deals.volume
					ELSE -deals.volume 
				END) as volume,
			SUM(CASE WHEN deals.type = $1 THEN deals.volume * deals.price
				ELSE -deals.volume * deals.price
			END) as total,
			AVG(deals.price) as price
		FROM deals as deals
		WHERE deals.clientID = $2 AND deals.ticker = $3
		GROUP BY deals.clientID, deals.ticker
		ON CONFLICT (clientID, ticker) DO UPDATE
			SET volume = EXCLUDED.volume, total = EXCLUDED.total, price = EXCLUDED.price`,
		"buy", clientID, ticker)
	if err != nil {
		return err
	}
	_, err = result.RowsAffected()
	if err != nil {
		return err
	}
	return nil
}
