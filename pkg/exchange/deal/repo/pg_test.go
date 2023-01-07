package repo

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	dealPkg "github.com/KeynihAV/exchange/pkg/exchange/deal"
	_ "github.com/jackc/pgx/v5/stdlib"
	sqlmock "gopkg.in/DATA-DOG/go-sqlmock.v2"
)

func TestExchangeDB_AddOrder(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("cant create mock: %s", err)
	}
	defer db.Close()

	type args struct {
		deal *dealPkg.Order
	}
	tests := []struct {
		name    string
		ed      *ExchangeDB
		args    args
		want    int64
		wantErr bool
		mockF   func(sqlmock.Sqlmock)
	}{
		{name: "Ошибка подготовки",
			ed:      &ExchangeDB{DB: db},
			args:    args{deal: &dealPkg.Order{}},
			want:    0,
			wantErr: true,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectPrepare(`INSERT INTO orders`).WillReturnError(fmt.Errorf("insert error"))
			},
		},
		{name: "Ошибка получения lastID",
			ed:      &ExchangeDB{DB: db},
			args:    args{deal: &dealPkg.Order{}},
			want:    0,
			wantErr: true,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectPrepare(`INSERT INTO orders`).WillReturnError(nil)
				s.ExpectQuery(`INSERT INTO orders`).WillReturnError(fmt.Errorf("insert error"))
			},
		},
		{name: "Успешный insert",
			ed:      &ExchangeDB{DB: db},
			args:    args{deal: &dealPkg.Order{}},
			want:    1,
			wantErr: false,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectPrepare(`INSERT INTO orders`).WillReturnError(nil)
				s.ExpectQuery(`INSERT INTO orders`).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mockF(mock)
			got, err := tt.ed.AddOrder(tt.args.deal)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExchangeDB.AddOrder() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ExchangeDB.AddOrder() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExchangeDB_DeleteOrder(t *testing.T) {
	type args struct {
		dealID int64
	}

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("cant create mock: %s", err)
	}
	defer db.Close()

	tests := []struct {
		name    string
		ed      *ExchangeDB
		args    args
		wantErr bool
		mockF   func(sqlmock.Sqlmock)
	}{
		{name: "Ошибка insert",
			ed:      &ExchangeDB{DB: db},
			args:    args{dealID: 1},
			wantErr: true,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectExec(`DELETE FROM orders WHERE`).WillReturnError(fmt.Errorf("delete error"))
			},
		},
		{name: "Успешный insert",
			ed:      &ExchangeDB{DB: db},
			args:    args{dealID: 1},
			wantErr: false,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectExec(`DELETE FROM orders WHERE`).WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mockF(mock)
			if err := tt.ed.DeleteOrder(tt.args.dealID); (err != nil) != tt.wantErr {
				t.Errorf("ExchangeDB.DeleteOrder() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExchangeDB_MarkDealShipped(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("cant create mock: %s", err)
	}
	defer db.Close()

	type args struct {
		dealID int64
	}
	tests := []struct {
		name    string
		ed      *ExchangeDB
		args    args
		wantErr bool
		mockF   func(sqlmock.Sqlmock)
	}{
		{name: "Ошибка update",
			ed:      &ExchangeDB{DB: db},
			args:    args{dealID: 1},
			wantErr: true,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectExec(`UPDATE deals SET shipped`).WillReturnError(fmt.Errorf("update error"))
			},
		},
		{name: "Ошибка rows affected",
			ed:      &ExchangeDB{DB: db},
			args:    args{dealID: 1},
			wantErr: true,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectExec(`UPDATE deals SET shipped`).WillReturnResult(sqlmock.NewErrorResult(fmt.Errorf("error result")))
			},
		},
		{name: "Успешный update",
			ed:      &ExchangeDB{DB: db},
			args:    args{dealID: 1},
			wantErr: false,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectExec(`UPDATE deals SET shipped`).WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mockF(mock)
			if err := tt.ed.MarkDealShipped(tt.args.dealID); (err != nil) != tt.wantErr {
				t.Errorf("ExchangeDB.MarkDealShipped() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExchangeDB_GetOrdersForClose(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("cant create mock: %s", err)
	}
	defer db.Close()

	type args struct {
		ticker string
		price  float32
	}
	tests := []struct {
		name    string
		ed      *ExchangeDB
		args    args
		want    []*dealPkg.Order
		wantErr bool
		mockF   func(sqlmock.Sqlmock)
	}{
		{name: "Ошибка select",
			ed:      &ExchangeDB{DB: db},
			args:    args{ticker: "123", price: 10},
			wantErr: true,
			want:    nil,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectQuery(`SELECT`).WillReturnError(fmt.Errorf("select error"))
			},
		},
		{name: "Ошибка scan",
			ed:      &ExchangeDB{DB: db},
			args:    args{ticker: "123", price: 10},
			wantErr: true,
			want:    nil,
			mockF: func(s sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "brokerID"}).AddRow(0, "one").AddRow(1, "two")
				s.ExpectQuery(`SELECT`).WillReturnRows(rows)
			},
		},
		{name: "Успешный select",
			ed:      &ExchangeDB{DB: db},
			args:    args{ticker: "ticker1", price: 10},
			wantErr: false,
			want: []*dealPkg.Order{{ID: 1, BrokerID: 1, ClientID: 1, Ticker: "ticker1",
				Volume: 10, Time: 12345678, Type: "buy", Price: 100, CompletedVolume: 8}},
			mockF: func(s sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "brokerid", "clientid", "ticker", "volume", "time", "type", "price", "completedVolume"}).
					AddRow(1, 1, 1, "ticker1", 10, 12345678, "buy", 100, 8)
				s.ExpectQuery(`SELECT`).WillReturnRows(rows)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mockF(mock)
			got, err := tt.ed.GetOrdersForClose(tt.args.ticker, tt.args.price)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExchangeDB.GetOrdersForClose() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ExchangeDB.GetOrdersForClose() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExchangeDB_MakeDeal(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("cant create mock: %s", err)
	}
	defer db.Close()

	type args struct {
		order         *dealPkg.Order
		volumeToClose int32
	}
	tNow := int32(time.Now().Unix())

	tests := []struct {
		name    string
		ed      *ExchangeDB
		args    args
		want    *dealPkg.Deal
		wantErr bool
		mockF   func(sqlmock.Sqlmock)
	}{
		{name: "Ошибка открытия транзакции",
			ed:      &ExchangeDB{DB: db},
			args:    args{order: &dealPkg.Order{}, volumeToClose: 1},
			wantErr: true,
			want:    nil,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectBegin().WillReturnError(fmt.Errorf("error begin"))
			},
		},
		{name: "Ошибка update",
			ed:      &ExchangeDB{DB: db},
			args:    args{order: &dealPkg.Order{ID: 1, Volume: 10}, volumeToClose: 1},
			wantErr: true,
			want:    nil,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectBegin()
				s.ExpectExec("UPDATE orders SET completedVolume").WillReturnError(fmt.Errorf("update error"))
			},
		},
		{name: "Ошибка rows affected",
			ed:      &ExchangeDB{DB: db},
			args:    args{order: &dealPkg.Order{ID: 1, Volume: 10}, volumeToClose: 1},
			wantErr: true,
			want:    nil,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectBegin()
				s.ExpectExec("UPDATE orders SET completedVolume").WillReturnResult(sqlmock.NewErrorResult(fmt.Errorf("error result")))
			},
		},
		{name: "Ошибка insert",
			ed:      &ExchangeDB{DB: db},
			args:    args{order: &dealPkg.Order{ID: 1, Volume: 10}, volumeToClose: 1},
			wantErr: true,
			want:    nil,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectBegin()
				s.ExpectExec("UPDATE orders SET completedVolume").WillReturnResult(sqlmock.NewResult(1, 1))
				s.ExpectPrepare("INSERT INTO deals").WillReturnError(fmt.Errorf("insert error"))
			},
		},
		{name: "Ошибка insert",
			ed:      &ExchangeDB{DB: db},
			args:    args{order: &dealPkg.Order{ID: 1, Volume: 10}, volumeToClose: 1},
			wantErr: true,
			want:    nil,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectBegin()
				s.ExpectExec("UPDATE orders SET completedVolume").WillReturnResult(sqlmock.NewResult(1, 1))
				s.ExpectPrepare("INSERT INTO deals").WillReturnError(nil)
				s.ExpectQuery(`INSERT INTO deals`).WillReturnError(fmt.Errorf("insert error"))
			},
		},
		{name: "Корректный update orders, insert deals",
			ed: &ExchangeDB{DB: db},
			args: args{order: &dealPkg.Order{ID: 1, BrokerID: 1, ClientID: 1,
				Ticker: "ticker1", Price: 100, Type: "sell", Volume: 100, CompletedVolume: 10, Time: tNow}, volumeToClose: 1},
			wantErr: false,
			want: &dealPkg.Deal{ID: 1, BrokerID: 1, ClientID: 1, OrderID: 1, Ticker: "ticker1", Volume: 1,
				Partial: true, Time: tNow, Price: 100, Type: "sell"},
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectBegin()
				s.ExpectExec("UPDATE orders SET completedVolume").WithArgs(10, 1).
					WillReturnResult(sqlmock.NewResult(1, 1))
				s.ExpectPrepare("INSERT INTO deals").WillReturnError(nil)
				s.ExpectQuery("INSERT INTO deals").
					WithArgs(1, 1, 1, "ticker1", 1, true, tNow, float64(100), "sell").
					WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
			},
		},
		{name: "Корректный delete orders, insert deals",
			ed: &ExchangeDB{DB: db},
			args: args{order: &dealPkg.Order{ID: 1, BrokerID: 1, ClientID: 1,
				Ticker: "ticker1", Price: 100, Type: "sell", Volume: 100, CompletedVolume: 100, Time: tNow}, volumeToClose: 100},
			wantErr: false,
			want: &dealPkg.Deal{ID: 1, BrokerID: 1, ClientID: 1, OrderID: 1, Ticker: "ticker1", Volume: 100,
				Partial: false, Time: tNow, Price: 100, Type: "sell"},
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectBegin()
				s.ExpectExec("DELETE FROM orders").WithArgs(1).
					WillReturnResult(sqlmock.NewResult(1, 1))
				s.ExpectPrepare("INSERT INTO deals").WillReturnError(nil)
				s.ExpectQuery("INSERT INTO deals").
					WithArgs(1, 1, 1, "ticker1", 100, false, tNow, float64(100), "sell").
					WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mockF(mock)
			got, err := tt.ed.MakeDeal(tt.args.order, tt.args.volumeToClose)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExchangeDB.MakeDeal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ExchangeDB.MakeDeal() = %v, want %v", got, tt.want)
			}
		})
	}
}
