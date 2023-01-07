package repo

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	statsPkg "github.com/KeynihAV/exchange/pkg/broker/stats"
	_ "github.com/jackc/pgx/v5/stdlib"
	"gopkg.in/DATA-DOG/go-sqlmock.v2"
)

func TestStatsRepo_Add(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("cant create mock: %s", err)
	}
	defer db.Close()

	type args struct {
		ohlcv *statsPkg.OHLCV
	}
	tests := []struct {
		name    string
		sr      *StatsRepo
		args    args
		wantErr bool
		mockF   func(sqlmock.Sqlmock)
	}{
		{name: "Ошибка insert",
			sr:      &StatsRepo{DB: db},
			args:    args{ohlcv: &statsPkg.OHLCV{}},
			wantErr: true,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectExec("INSERT INTO stats").WillReturnError(fmt.Errorf("insert error"))
			},
		},
		{name: "Ошибка result",
			sr:      &StatsRepo{DB: db},
			args:    args{ohlcv: &statsPkg.OHLCV{}},
			wantErr: true,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectExec("INSERT INTO stats").WillReturnResult(sqlmock.NewErrorResult(fmt.Errorf("result error")))
			},
		},
		{name: "Ошибка result 2",
			sr:      &StatsRepo{DB: db},
			args:    args{ohlcv: &statsPkg.OHLCV{}},
			wantErr: true,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectExec("INSERT INTO stats").WillReturnResult(sqlmock.NewResult(1, 0))
			},
		},
		{name: "Успешный insert",
			sr:      &StatsRepo{DB: db},
			args:    args{ohlcv: &statsPkg.OHLCV{Time: time.Now(), TimeInt: int32(time.Now().Unix()), Interval: 1, Open: 10, High: 12, Low: 5, Close: 6, Volume: 100, Ticker: "ticker1"}},
			wantErr: false,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectExec("INSERT INTO stats").WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
	}
	for _, tt := range tests {
		tt.mockF(mock)
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.sr.Add(tt.args.ohlcv); (err != nil) != tt.wantErr {
				t.Errorf("StatsRepo.Add() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestStatsRepo_GeStatsByTicker(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("cant create mock: %s", err)
	}
	defer db.Close()

	type args struct {
		ticker string
	}
	currTime := time.Now()
	tests := []struct {
		name    string
		sr      *StatsRepo
		args    args
		want    []*statsPkg.OHLCV
		wantErr bool
		mockF   func(sqlmock.Sqlmock)
	}{
		{name: "Ошибка select",
			sr:      &StatsRepo{DB: db},
			args:    args{ticker: "ticker1"},
			wantErr: true,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectQuery("SELECT").WithArgs("ticker1", sqlmock.AnyArg()).WillReturnError(fmt.Errorf("select error"))
			},
		},
		{name: "Ошибка scan",
			sr:      &StatsRepo{DB: db},
			args:    args{ticker: "ticker1"},
			wantErr: true,
			mockF: func(s sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "brokerID"}).AddRow(0, "one").AddRow(1, "two")
				s.ExpectQuery(`SELECT`).WillReturnRows(rows)
			},
		},
		{name: "Корректный select",
			sr:      &StatsRepo{DB: db},
			args:    args{ticker: "ticker1"},
			want:    []*statsPkg.OHLCV{{Time: currTime, Open: 10, High: 20, Low: 10, Close: 15, Volume: 100, Ticker: "ticker1"}},
			wantErr: false,
			mockF: func(s sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"time", "open", "high", "low", "close", "volume", "ticker"}).
					AddRow(currTime, 10, 20, 10, 15, 100, "ticker1")
				s.ExpectQuery(`SELECT`).WithArgs("ticker1", sqlmock.AnyArg()).WillReturnRows(rows)
			},
		},
	}
	for _, tt := range tests {
		tt.mockF(mock)
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.sr.GeStatsByTicker(tt.args.ticker)
			if (err != nil) != tt.wantErr {
				t.Errorf("StatsRepo.GeStatsByTicker() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StatsRepo.GeStatsByTicker() = %v, want %v", got, tt.want)
			}
		})
	}
}
