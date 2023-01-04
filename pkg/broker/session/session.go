package session

type Session struct {
	ID        string
	UserID    int64
	ExpiresAt int64
}
