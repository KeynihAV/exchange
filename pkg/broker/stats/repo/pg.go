package repo

import (
	"database/sql"
	"fmt"

	dealDeliveryPkg "github.com/KeynihAV/exchange/pkg/exchange/deal/delivery"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type StatsRepo struct {
	DB *sql.DB
}

func NewStatsRepo(db *sql.DB) (*StatsRepo, error) {
	_, err := db.Exec(
		`CREATE TABLE IF NOT EXISTS stats(
			id SERIAL PRIMARY KEY,			
			time int NOT NULL,
			interval int NOT NULL,
			open float8 NOT NULL,
			high float8 NOT NULL,
			low float8 NOT NULL,
			close float8 NOT NULL,
			volume int NOT NULL,
			ticker varchar(150));
		CREATE INDEX IF NOT EXISTS ticker_idx ON stats (ticker);`)
	if err != nil {
		return nil, err
	}

	return &StatsRepo{
		DB: db,
	}, nil
}

func (sr *StatsRepo) Add(ohlcv *dealDeliveryPkg.OHLCV) error {
	result, err := sr.DB.Exec(`INSERT INTO stats(time, interval, open, high, low, close, volume, ticker) 
		values($1, $2, $3, $4, $5, $6, $7, $8)`,
		ohlcv.Time, ohlcv.Interval, ohlcv.Open, ohlcv.High, ohlcv.Low, ohlcv.Close, ohlcv.Volume, ohlcv.Ticker)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("not insert row in stats")
	}

	return nil
}
