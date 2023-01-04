package repo

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	sessionsPkg "github.com/KeynihAV/exchange/pkg/broker/session"
	configPkg "github.com/KeynihAV/exchange/pkg/config"

	"github.com/gomodule/redigo/redis"
)

type SessionsDB struct {
	Conn redis.Conn
}

func NewDB(config *configPkg.Config) (*SessionsDB, error) {
	redisConn, err := redis.DialURL(config.Redis.Addr)
	if err != nil {
		return nil, err
	}

	return &SessionsDB{
		Conn: redisConn,
	}, nil
}

func (sr *SessionsDB) Add(session *sessionsPkg.Session) error {
	dataSerialized, _ := json.Marshal(session)
	mkey := "sessions_" + strconv.Itoa(int(session.UserID))
	data, err := sr.Conn.Do("SET", mkey, dataSerialized, "EX", session.ExpiresAt-time.Now().Unix())
	result, err := redis.String(data, err)
	if err != nil {
		return err
	}
	if result != "OK" {
		return fmt.Errorf("result not OK")
	}
	return nil
}

func (sr *SessionsDB) Get(userID int64) (*sessionsPkg.Session, error) {
	key := "sessions_" + strconv.Itoa(int(userID))
	data, err := redis.Bytes(sr.Conn.Do("GET", key))
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, fmt.Errorf("сессия не найдена")
	}
	sess := &sessionsPkg.Session{}
	err = json.Unmarshal(data, sess)
	if err != nil {
		return nil, err
	}

	return sess, nil
}
