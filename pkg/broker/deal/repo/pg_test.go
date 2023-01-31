package repo

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	dealPkg "github.com/KeynihAV/exchange/pkg/exchange/deal"
	_ "github.com/jackc/pgx/v5/stdlib"
	"gopkg.in/DATA-DOG/go-sqlmock.v2"
)

func TestDealRepo_AddOrder(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("cant create mock: %s", err)
	}
	defer db.Close()

	type args struct {
		order *dealPkg.Order
	}

	tests := []struct {
		name    string
		dr      *DealRepo
		args    args
		want    int64
		wantErr bool
		mockF   func(sqlmock.Sqlmock)
	}{
		{name: "Ошибка insert",
			dr:      &DealRepo{DB: db},
			args:    args{&dealPkg.Order{}},
			wantErr: true,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectPrepare(`INSERT INTO orders`).WillReturnError(fmt.Errorf("insert error"))
			},
		},
		{name: "Ошибка result",
			dr:      &DealRepo{DB: db},
			args:    args{&dealPkg.Order{}},
			wantErr: true,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectPrepare(`INSERT INTO orders`).WillReturnError(nil)
				s.ExpectQuery(`INSERT INTO orders`).WillReturnError(fmt.Errorf("insert error"))
			},
		},
		{name: "Успешный insert",
			dr:      &DealRepo{DB: db},
			args:    args{&dealPkg.Order{BrokerID: 1, ClientID: 1, Ticker: "ticker1", Volume: 10, Time: int32(time.Now().Unix()), Price: 100, Type: "sell"}},
			wantErr: false,
			want:    1,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectPrepare(`INSERT INTO orders`).WillReturnError(nil)
				s.ExpectQuery(`INSERT INTO orders`).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mockF(mock)
			got, err := tt.dr.AddOrder(tt.args.order)
			if (err != nil) != tt.wantErr {
				t.Errorf("DealRepo.AddOrder() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("DealRepo.AddOrder() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDealRepo_DeleteOrder(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("cant create mock: %s", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	tx1, err := db.BeginTx(context.TODO(), nil)
	if err != nil {
		t.Errorf("tx error %v", err)
	}
	mock.ExpectBegin()
	tx2, err := db.BeginTx(context.TODO(), nil)
	if err != nil {
		t.Errorf("tx error %v", err)
	}

	type args struct {
		id int64
		tx *sql.Tx
	}
	tests := []struct {
		name    string
		dr      *DealRepo
		args    args
		wantErr bool
		mockF   func(sqlmock.Sqlmock)
	}{
		{name: "Ошибка delete",
			dr:      &DealRepo{DB: db},
			args:    args{id: 1, tx: tx1},
			wantErr: true,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectExec(`DELETE FROM orders WHERE`).WillReturnError(fmt.Errorf("delete error"))
			},
		},
		{name: "Успешный delete",
			dr:      &DealRepo{DB: db},
			args:    args{id: 1, tx: tx2},
			wantErr: false,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectExec(`DELETE FROM orders WHERE`).WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mockF(mock)
			if err := tt.dr.DeleteOrder(tt.args.id, tt.args.tx); (err != nil) != tt.wantErr {
				t.Errorf("DealRepo.DeleteOrder() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDealRepo_OrdersByClient(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("cant create mock: %s", err)
	}
	defer db.Close()

	type args struct {
		clientID int
	}
	tests := []struct {
		name    string
		dr      *DealRepo
		args    args
		want    []*dealPkg.Order
		wantErr bool
		mockF   func(sqlmock.Sqlmock)
	}{
		{name: "Ошибка select",
			dr:      &DealRepo{DB: db},
			args:    args{clientID: 1},
			wantErr: true,
			want:    nil,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectQuery(`SELECT`).WillReturnError(fmt.Errorf("select error"))
			},
		},
		{name: "Ошибка scan",
			dr:      &DealRepo{DB: db},
			args:    args{clientID: 1},
			wantErr: true,
			want:    nil,
			mockF: func(s sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "brokerID"}).AddRow(0, "one").AddRow(1, "two")
				s.ExpectQuery(`SELECT`).WillReturnRows(rows)
			},
		},
		{name: "Успешный select",
			dr:      &DealRepo{DB: db},
			args:    args{clientID: 1},
			wantErr: false,
			want: []*dealPkg.Order{{ID: 1, BrokerID: 1, ClientID: 1, Ticker: "ticker1",
				Volume: 10, Time: 12345678, Type: "buy", Price: 100, CompletedVolume: 4}},
			mockF: func(s sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "brokerid", "clientid", "ticker", "volume", "completedVolume", "time", "price", "type"}).
					AddRow(1, 1, 1, "ticker1", 10, 4, 12345678, 100, "buy")
				s.ExpectQuery(`SELECT`).WillReturnRows(rows)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mockF(mock)
			got, err := tt.dr.OrdersByClient(tt.args.clientID)
			if (err != nil) != tt.wantErr {
				t.Errorf("DealRepo.OrdersByClient() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DealRepo.OrdersByClient() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDealRepo_GetExchangeID(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("cant create mock: %s", err)
	}
	defer db.Close()

	type args struct {
		orderID int64
	}
	tests := []struct {
		name    string
		dr      *DealRepo
		args    args
		want    int64
		wantErr bool
		mockF   func(sqlmock.Sqlmock)
	}{
		{name: "Ошибка scan",
			dr:      &DealRepo{DB: db},
			args:    args{orderID: 1},
			wantErr: true,
			want:    0,
			mockF: func(s sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id"}).AddRow(1)
				s.ExpectQuery(`SELECT`).WillReturnRows(rows).WillReturnError(fmt.Errorf("scan error"))
			},
		},
		{name: "Успешный select",
			dr:      &DealRepo{DB: db},
			args:    args{orderID: 1},
			wantErr: false,
			want:    1,
			mockF: func(s sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id"}).AddRow(1)
				s.ExpectQuery(`SELECT`).WillReturnRows(rows)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mockF(mock)
			got, err := tt.dr.GetExchangeID(tt.args.orderID)
			if (err != nil) != tt.wantErr {
				t.Errorf("DealRepo.GetExchangeID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("DealRepo.GetExchangeID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDealRepo_GetOrderID(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("cant create mock: %s", err)
	}
	defer db.Close()

	type args struct {
		exchangeID int64
	}
	tests := []struct {
		name    string
		dr      *DealRepo
		args    args
		want    int64
		wantErr bool
		mockF   func(sqlmock.Sqlmock)
	}{
		{name: "Ошибка scan",
			dr:      &DealRepo{DB: db},
			args:    args{exchangeID: 1},
			wantErr: true,
			want:    0,
			mockF: func(s sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id"}).AddRow(1)
				s.ExpectQuery(`SELECT`).WillReturnRows(rows).WillReturnError(fmt.Errorf("scan error"))
			},
		},
		{name: "Успешный select",
			dr:      &DealRepo{DB: db},
			args:    args{exchangeID: 1},
			wantErr: false,
			want:    1,
			mockF: func(s sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id"}).AddRow(1)
				s.ExpectQuery(`SELECT`).WillReturnRows(rows)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mockF(mock)
			got, err := tt.dr.GetOrderID(tt.args.exchangeID)
			if (err != nil) != tt.wantErr {
				t.Errorf("DealRepo.GetOrderID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("DealRepo.GetOrderID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDealRepo_MarkOrderShipped(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("cant create mock: %s", err)
	}
	defer db.Close()

	type args struct {
		id         int64
		exchangeID int64
	}

	tests := []struct {
		name    string
		dr      *DealRepo
		args    args
		wantErr bool
		mockF   func(sqlmock.Sqlmock)
	}{
		{name: "Ошибка update",
			dr:      &DealRepo{DB: db},
			args:    args{id: 1, exchangeID: 1},
			wantErr: true,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectExec(`UPDATE orders SET exchangeID`).WillReturnError(fmt.Errorf("update error"))
			},
		},
		{name: "Ошибка rows affected",
			dr:      &DealRepo{DB: db},
			args:    args{id: 1, exchangeID: 1},
			wantErr: true,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectExec(`UPDATE orders SET exchangeID`).WillReturnResult(sqlmock.NewErrorResult(fmt.Errorf("error result")))
			},
		},
		{name: "Успешный update",
			dr:      &DealRepo{DB: db},
			args:    args{id: 1, exchangeID: 1},
			wantErr: false,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectExec(`UPDATE orders SET exchangeID`).WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
	}
	for _, tt := range tests {
		tt.mockF(mock)
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.dr.MarkOrderShipped(tt.args.id, tt.args.exchangeID); (err != nil) != tt.wantErr {
				t.Errorf("DealRepo.MarkOrderShipped() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDealRepo_WriteDeal(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("cant create mock: %s", err)
	}
	defer db.Close()

	type args struct {
		deal *dealPkg.Deal
		tx   *sql.Tx
	}

	mock.ExpectBegin()
	tx1, err := db.BeginTx(context.TODO(), nil)
	if err != nil {
		t.Errorf("tx error %v", err)
	}

	tests := []struct {
		name    string
		dr      *DealRepo
		args    args
		wantErr bool
		mockF   func(sqlmock.Sqlmock)
	}{
		{name: "Ошибка insert",
			dr:      &DealRepo{DB: db},
			args:    args{deal: &dealPkg.Deal{}, tx: tx1},
			wantErr: true,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectExec(`INSERT INTO deals`).WillReturnError(fmt.Errorf("insert error"))
			},
		},
		{name: "Ошибка rows affected",
			dr:      &DealRepo{DB: db},
			args:    args{deal: &dealPkg.Deal{}, tx: tx1},
			wantErr: true,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectExec(`INSERT INTO deals`).WillReturnResult(sqlmock.NewErrorResult(fmt.Errorf("error result")))
			},
		},
		{name: "Успешный insert",
			dr:      &DealRepo{DB: db},
			args:    args{deal: &dealPkg.Deal{ID: 1, ClientID: 1, Ticker: "ticker1", Volume: 100, Partial: false, Type: "sell", OrderID: 1}, tx: tx1},
			wantErr: false,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectExec(`INSERT INTO deals`).WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
	}
	for _, tt := range tests {
		tt.mockF(mock)
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.dr.WriteDeal(tt.args.deal, tt.args.tx); (err != nil) != tt.wantErr {
				t.Errorf("DealRepo.WriteDeal() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDealRepo_UpdateOrderClosedVolume(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("cant create mock: %s", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	tx1, err := db.BeginTx(context.TODO(), nil)
	if err != nil {
		t.Errorf("tx error %v", err)
	}

	type args struct {
		orderID         int64
		completedVolume int32
		tx              *sql.Tx
	}
	tests := []struct {
		name    string
		dr      *DealRepo
		args    args
		wantErr bool
		mockF   func(sqlmock.Sqlmock)
	}{
		{name: "Ошибка update",
			dr:      &DealRepo{DB: db},
			args:    args{orderID: 1, tx: tx1},
			wantErr: true,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectExec(`UPDATE orders SET completedVolume`).WillReturnError(fmt.Errorf("update error"))
			},
		},
		{name: "Ошибка rows affected",
			dr:      &DealRepo{DB: db},
			args:    args{orderID: 1, tx: tx1},
			wantErr: true,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectExec(`UPDATE orders SET completedVolume`).WillReturnResult(sqlmock.NewErrorResult(fmt.Errorf("error result")))
			},
		},
		{name: "Успешный update",
			dr:      &DealRepo{DB: db},
			args:    args{orderID: 1, tx: tx1},
			wantErr: false,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectExec(`UPDATE orders SET completedVolume`).WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
	}
	for _, tt := range tests {
		tt.mockF(mock)
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.dr.UpdateOrderClosedVolume(tt.args.orderID, tt.args.completedVolume, tt.args.tx); (err != nil) != tt.wantErr {
				t.Errorf("DealRepo.UpdateOrderClosedVolume() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDealRepo_OrderClosedVolume(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("cant create mock: %s", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	tx1, err := db.BeginTx(context.TODO(), nil)
	if err != nil {
		t.Errorf("tx error %v", err)
	}

	type args struct {
		exchangeOrderID int64
		tx              *sql.Tx
	}
	tests := []struct {
		name    string
		dr      *DealRepo
		args    args
		want    int32
		wantErr bool
		mockF   func(sqlmock.Sqlmock)
	}{
		{name: "Ошибка scan",
			dr:      &DealRepo{DB: db},
			args:    args{exchangeOrderID: 1, tx: tx1},
			wantErr: true,
			want:    0,
			mockF: func(s sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id"}).AddRow(1)
				s.ExpectQuery(`SELECT`).WillReturnRows(rows).WillReturnError(fmt.Errorf("scan error"))
			},
		},
		{name: "Успешный select",
			dr:      &DealRepo{DB: db},
			args:    args{exchangeOrderID: 1, tx: tx1},
			wantErr: false,
			want:    10,
			mockF: func(s sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"volume"}).AddRow(10)
				s.ExpectQuery(`SELECT`).WillReturnRows(rows)
			},
		},
	}
	for _, tt := range tests {
		tt.mockF(mock)
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.dr.OrderClosedVolume(tt.args.exchangeOrderID, tt.args.tx)
			if (err != nil) != tt.wantErr {
				t.Errorf("DealRepo.OrderClosedVolume() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("DealRepo.OrderClosedVolume() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDealRepo_UpdatePositionsByClientAndTicker(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("cant create mock: %s", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	tx1, err := db.BeginTx(context.TODO(), nil)
	if err != nil {
		t.Errorf("tx error %v", err)
	}

	type args struct {
		clientID int32
		ticker   string
		tx       *sql.Tx
	}
	tests := []struct {
		name    string
		dr      *DealRepo
		args    args
		wantErr bool
		mockF   func(sqlmock.Sqlmock)
	}{
		{name: "Ошибка update",
			dr:      &DealRepo{DB: db},
			args:    args{clientID: 1, ticker: "ticker1", tx: tx1},
			wantErr: true,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectExec(`INSERT INTO positions`).WillReturnError(fmt.Errorf("update error"))
			},
		},
		{name: "Ошибка rows affected",
			dr:      &DealRepo{DB: db},
			args:    args{clientID: 1, ticker: "ticker1", tx: tx1},
			wantErr: true,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectExec(`INSERT INTO positions`).WillReturnResult(sqlmock.NewErrorResult(fmt.Errorf("error result")))
			},
		},
		{name: "Успешный update",
			dr:      &DealRepo{DB: db},
			args:    args{clientID: 1, ticker: "ticker1", tx: tx1},
			wantErr: false,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectExec(`INSERT INTO positions`).WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
	}
	for _, tt := range tests {
		tt.mockF(mock)
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.dr.UpdatePositionsByClientAndTicker(tt.args.clientID, tt.args.ticker, tt.args.tx); (err != nil) != tt.wantErr {
				t.Errorf("DealRepo.UpdatePositionsByClientAndTicker() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
