package events

import "time"

type UserSignup struct {
	Event   string    `json:"event"`   // "user_registered"
	Version int       `json:"version"` // 1
	UserID  string    `json:"user_id"`
	Email   string    `json:"email"`
	TS      time.Time `json:"ts"`
}
