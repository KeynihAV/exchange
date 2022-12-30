package usecase

import (
	"database/sql"

	clientPkg "github.com/KeynihAV/exchange/pkg/broker/client"
	clientRepoPkg "github.com/KeynihAV/exchange/pkg/broker/client/repo"
)

type ClientsManager struct {
	CR            *clientRepoPkg.ClientsRepo
	ActiveDialogs map[int64]*clientPkg.Dialog
}

func NewClientsManager(db *sql.DB) (*ClientsManager, error) {
	cr, err := clientRepoPkg.NewClientsRepo(db)
	if err != nil {
		return nil, err
	}
	return &ClientsManager{
		CR:            cr,
		ActiveDialogs: make(map[int64]*clientPkg.Dialog),
	}, nil
}

func (cm *ClientsManager) CheckAndCreateClient(login string, ID int) (*clientPkg.Client, error) {
	var client *clientPkg.Client

	clients, err := cm.CR.GetByIDs(ID)
	if err != nil {
		return nil, err
	}
	if len(clients) == 0 {
		client = &clientPkg.Client{Login: login, TgID: ID}
		err = cm.CR.Add(client)
		if err != nil {
			return nil, err
		}
	} else {
		client = clients[ID]
	}

	return client, nil
}

func (cm *ClientsManager) GetBalance(client *clientPkg.Client) ([]*clientPkg.Position, error) {
	return cm.CR.GetBalance(client)
}
