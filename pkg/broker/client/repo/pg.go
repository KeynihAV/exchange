package repo

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"

	clientPkg "github.com/KeynihAV/exchange/pkg/broker/client"
)

type ClientsRepo struct {
	DB *sql.DB
}

func NewClientsRepo(db *sql.DB) (*ClientsRepo, error) {
	_, err := db.Exec(
		`CREATE TABLE IF NOT EXISTS clients(
			id SERIAL PRIMARY KEY,			
			login varchar(200) NOT NULL,
			tgID bigint NOT NULL,
			chatID bigint NOT NULL,
			balance float8 NOT NULL);
		CREATE UNIQUE INDEX IF NOT EXISTS tgID_idx ON clients (tgID);`)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(
		`CREATE TABLE IF NOT EXISTS positions(
			id SERIAL PRIMARY KEY,
			clientID int NOT NULL,
			ticker varchar(200) NOT NULL,
			volume int NOT NULL,			
			price float8 NOT NULL,
			total float8 NOT NULL);
		CREATE UNIQUE INDEX IF NOT EXISTS client_idx ON positions (clientID, ticker);`)
	if err != nil {
		return nil, err
	}

	return &ClientsRepo{DB: db}, nil
}

func (cr *ClientsRepo) GetByIDs(ids ...int64) (map[int64]*clientPkg.Client, error) {
	params := []string{}
	values := []interface{}{}
	i := 1
	for _, v := range ids {
		params = append(params, "$"+strconv.Itoa(i))
		values = append(values, v)
		i++
	}
	queryString := fmt.Sprintf(`SELECT id, tgID, login, chatID, balance FROM clients WHERE tgID IN (%v)`, strings.Join(params, ","))
	result, err := cr.DB.Query(queryString, values...)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	clients := make(map[int64]*clientPkg.Client)
	for result.Next() {
		client := &clientPkg.Client{}
		err = result.Scan(&client.ID, &client.TgID, &client.Login, &client.ChatID, &client.Balance)
		if err != nil {
			return nil, err
		}
		clients[client.TgID] = client
	}

	return clients, nil
}

func (cr *ClientsRepo) Add(client *clientPkg.Client) error {
	result, err := cr.DB.Exec(`INSERT INTO clients(tgID, login, chatID, balance) values($1, $2, $3, $4)`,
		client.TgID, client.Login, client.ChatID, 0)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("not insert row in clients")
	}

	return nil
}

func (cr *ClientsRepo) GetBalance(clientID int) ([]*clientPkg.Position, error) {
	result, err := cr.DB.Query(`SELECT id, clientID, ticker, volume, price, total
		 FROM positions WHERE clientID = $1`, clientID)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	positions := make([]*clientPkg.Position, 0)
	for result.Next() {
		position := &clientPkg.Position{}
		err = result.Scan(&position.ID, &position.ClientID, &position.Ticker, &position.Volume, &position.Price, &position.Total)
		if err != nil {
			return nil, err
		}
		positions = append(positions, position)
	}

	return positions, nil
}
