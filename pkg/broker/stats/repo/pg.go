package repo

import (
	"database/sql"
	"fmt"
	"time"

	statsPkg "github.com/KeynihAV/exchange/pkg/broker/stats"
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

func (sr *StatsRepo) Add(ohlcv *statsPkg.OHLCV) error {
	result, err := sr.DB.Exec(`INSERT INTO stats(time, interval, open, high, low, close, volume, ticker) 
		values($1, $2, $3, $4, $5, $6, $7, $8)`,
		ohlcv.TimeInt, ohlcv.Interval, ohlcv.Open, ohlcv.High, ohlcv.Low, ohlcv.Close, ohlcv.Volume, ohlcv.Ticker)
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

func (sr *StatsRepo) GeStatsByTicker(ticker string) ([]*statsPkg.OHLCV, error) {
	result, err := sr.DB.Query(`
		SELECT 
			date_trunc('minute', to_timestamp(time)) as time, 
			MIN(open), MAX(high), MIN(low), MIN(close), SUM(volume), ticker
		FROM stats 
		WHERE ticker = $1 AND time > $2
		GROUP BY date_trunc('minute', to_timestamp(time)), ticker
		ORDER BY time;`, ticker, time.Now().Add(time.Minute*-5).Unix())
	if err != nil {
		return nil, err
	}
	defer result.Close()

	stats := make([]*statsPkg.OHLCV, 0)
	for result.Next() {
		ohlcv := &statsPkg.OHLCV{}
		err = result.Scan(&ohlcv.Time, &ohlcv.Open, &ohlcv.High, &ohlcv.Low, &ohlcv.Close, &ohlcv.Volume, &ohlcv.Ticker)
		if err != nil {
			return nil, err
		}
		stats = append(stats, ohlcv)
	}

	return stats, nil
}
