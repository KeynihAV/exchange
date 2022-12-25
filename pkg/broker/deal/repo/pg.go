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
		CREATE UNIQUE INDEX IF NOT EXISTS exchangeID_idx ON orders (exchangeID);`)
	if err != nil {
		return nil, err
	}

	return &DealRepo{
		DB: db,
	}, nil
}

func (dr *DealRepo) AddOrder(deal *dealPkg.Order) (int64, error) {
	query := `INSERT INTO orders(brokerID, clientID, ticker, volume, completedVolume, time, price, type)
	values($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id;`

	statement, err := dr.DB.Prepare(query)
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
