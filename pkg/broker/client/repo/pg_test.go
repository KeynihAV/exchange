package repo

import (
	"fmt"
	"reflect"
	"testing"

	clientPkg "github.com/KeynihAV/exchange/pkg/broker/client"
	_ "github.com/jackc/pgx/v5/stdlib"
	"gopkg.in/DATA-DOG/go-sqlmock.v2"
)

func TestClientsRepo_GetByIDs(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("cant create mock: %s", err)
	}
	defer db.Close()
	resultMap := make(map[int64]*clientPkg.Client)
	resultMap[1] = &clientPkg.Client{ID: 1, TgID: 1, Login: "user1", ChatID: 1, Balance: 100}

	type args struct {
		ids []int64
	}
	tests := []struct {
		name    string
		cr      *ClientsRepo
		args    args
		want    map[int64]*clientPkg.Client
		wantErr bool
		mockF   func(sqlmock.Sqlmock)
	}{
		{name: "Ошибка select",
			cr:      &ClientsRepo{DB: db},
			args:    args{ids: []int64{1}},
			wantErr: true,
			want:    nil,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectQuery(`SELECT`).WillReturnError(fmt.Errorf("select error"))
			},
		},
		{name: "Ошибка scan",
			cr:      &ClientsRepo{DB: db},
			args:    args{ids: []int64{1}},
			wantErr: true,
			want:    nil,
			mockF: func(s sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "brokerID"}).AddRow(0, "one").AddRow(1, "two")
				s.ExpectQuery(`SELECT`).WillReturnRows(rows)
			},
		},
		{name: "Успешный select",
			cr:      &ClientsRepo{DB: db},
			args:    args{ids: []int64{1}},
			wantErr: false,
			want:    resultMap,
			mockF: func(s sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "tgID", "login", "ticker", "chatID"}).
					AddRow(1, 1, "user1", 1, 100)
				s.ExpectQuery(`SELECT`).WillReturnRows(rows)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mockF(mock)
			got, err := tt.cr.GetByIDs(tt.args.ids...)
			if (err != nil) != tt.wantErr {
				t.Errorf("ClientsRepo.GetByIDs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ClientsRepo.GetByIDs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClientsRepo_Add(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("cant create mock: %s", err)
	}
	defer db.Close()

	type args struct {
		client *clientPkg.Client
	}
	tests := []struct {
		name    string
		cr      *ClientsRepo
		args    args
		wantErr bool
		mockF   func(sqlmock.Sqlmock)
	}{
		{name: "Ошибка insert",
			cr:      &ClientsRepo{DB: db},
			args:    args{&clientPkg.Client{}},
			wantErr: true,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectExec(`INSERT INTO clients`).WillReturnError(fmt.Errorf("insert error"))
			},
		},
		{name: "Ошибка result",
			cr:      &ClientsRepo{DB: db},
			args:    args{&clientPkg.Client{}},
			wantErr: true,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectExec(`INSERT INTO client`).WillReturnResult(sqlmock.NewErrorResult(fmt.Errorf("error result")))
			},
		},
		{name: "Успешный insert",
			cr:      &ClientsRepo{DB: db},
			args:    args{&clientPkg.Client{ID: 1, TgID: 1, Login: "user1", ChatID: 1, Balance: 100}},
			wantErr: false,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectExec(`INSERT INTO clients`).WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
	}
	for _, tt := range tests {
		tt.mockF(mock)
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.cr.Add(tt.args.client); (err != nil) != tt.wantErr {
				t.Errorf("ClientsRepo.Add() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClientsRepo_GetBalance(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("cant create mock: %s", err)
	}
	defer db.Close()

	type args struct {
		client *clientPkg.Client
	}
	tests := []struct {
		name    string
		cr      *ClientsRepo
		args    args
		want    []*clientPkg.Position
		wantErr bool
		mockF   func(sqlmock.Sqlmock)
	}{
		{name: "Ошибка select",
			cr:      &ClientsRepo{DB: db},
			args:    args{&clientPkg.Client{}},
			wantErr: true,
			want:    nil,
			mockF: func(s sqlmock.Sqlmock) {
				s.ExpectQuery(`SELECT`).WillReturnError(fmt.Errorf("select error"))
			},
		},
		{name: "Ошибка scan",
			cr:      &ClientsRepo{DB: db},
			args:    args{&clientPkg.Client{}},
			wantErr: true,
			want:    nil,
			mockF: func(s sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "brokerID"}).AddRow(0, "one").AddRow(1, "two")
				s.ExpectQuery(`SELECT`).WillReturnRows(rows)
			},
		},
		{name: "Успешный select",
			cr:      &ClientsRepo{DB: db},
			args:    args{&clientPkg.Client{}},
			wantErr: false,
			want:    []*clientPkg.Position{{ID: 1, ClientID: 1, Ticker: "ticker1", Volume: 10, Price: 100, Total: 1000}},
			mockF: func(s sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "clientid", "ticker", "volume", "price", "total"}).
					AddRow(1, 1, "ticker1", 10, 100, 1000)
				s.ExpectQuery(`SELECT`).WillReturnRows(rows)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mockF(mock)
			got, err := tt.cr.GetBalance(tt.args.client.ID)
			if (err != nil) != tt.wantErr {
				t.Errorf("ClientsRepo.GetBalance() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ClientsRepo.GetBalance() = %v, want %v", got, tt.want)
			}
		})
	}
}
