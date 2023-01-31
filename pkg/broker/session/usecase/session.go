package usecase

import (
	"fmt"
	"time"

	sessionsPkg "github.com/KeynihAV/exchange/pkg/broker/session"
	sessionsRepoPkg "github.com/KeynihAV/exchange/pkg/broker/session/repo"
	"github.com/KeynihAV/exchange/pkg/config"

	jwt "github.com/dgrijalva/jwt-go"
)

type SessKey string

type JwtClaims struct {
	User UserClaims `json:"user"`
	jwt.StandardClaims
}

type UserClaims struct {
	UserName string `json:"username"`
	UserID   string `json:"id"`
}

type SessionsManager struct {
	Repo SessRepo
}

type SessRepo interface {
	Add(session *sessionsPkg.Session) error
	Get(userID int64) (*sessionsPkg.Session, error)
}

var (
	TokenSecret = []byte("der parol")
)

func NewSessionsManager(config *config.Config) (*SessionsManager, error) {
	sessDB, err := sessionsRepoPkg.NewDB(config)
	if err != nil {
		return nil, err
	}
	return &SessionsManager{
		Repo: sessDB,
	}, nil
}

func ParseSecretGetter(token *jwt.Token) (interface{}, error) {
	method, ok := token.Method.(*jwt.SigningMethodHMAC)
	if !ok || method.Alg() != "HS256" {
		return nil, fmt.Errorf("bad sign method")
	}
	return TokenSecret, nil
}

func (sm *SessionsManager) GetSession(userID int64) (*sessionsPkg.Session, error) {
	sess, err := sm.Repo.Get(userID)
	if err != nil {
		return nil, err
	}

	return sess, nil
}

func (sm *SessionsManager) CreateSession(userID int64, expiresAt time.Time) (string, error) {

	newSession := &sessionsPkg.Session{
		UserID:    userID,
		ExpiresAt: expiresAt.Unix(),
	}

	err := sm.Repo.Add(newSession)
	if err != nil {
		return "", err
	}

	return "tokenString", nil
}
