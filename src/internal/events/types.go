// types.go (unchanged, as no issues found)
package events

import "time"

type UserSignin struct {
	Event   string    `json:"event"`   // "user_registered"
	Version int       `json:"version"` // 1
	UserID  string    `json:"user_id"`
	Email   string    `json:"email"`
	TS      time.Time `json:"ts"`
}
