package apis

import (
	"fmt"

	"github.com/dolphindb/api-go/api"
)

// Login checks whether the Login api is valid.
func Login(db api.DolphinDB) error {
	l := new(api.LoginRequest).
		SetUserID(User).
		SetPassword(Password)
	err := db.Login(l)
	fmt.Println("Login")
	return err
}

// Logout checks whether the Logout api is valid.
func Logout(db api.DolphinDB) error {
	err := db.Logout()
	fmt.Println("Logout")
	return err
}
