package apis

import (
	"github.com/dolphindb/api-go/v3/api"
	"github.com/dolphindb/api-go/v3/logging"
)

// Login checks whether the Login api is valid.
func Login(db api.DolphinDB) error {
	l := new(api.LoginRequest).
		SetUserID(User).
		SetPassword(Password)
	err := db.Login(l)
	logging.Info("example.apis", "login")
	return err
}

// Logout checks whether the Logout api is valid.
func Logout(db api.DolphinDB) error {
	err := db.Logout()
	logging.Info("example.apis", "logout")
	return err
}
