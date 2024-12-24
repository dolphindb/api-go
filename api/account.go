package api

import (
	"github.com/dolphindb/api-go/v3/dialer"
)

// AccountAPI interface declares apis about account.
type AccountAPI interface {
	// Login dolphindb.
	// See DolphinDB function `login`: https://www.dolphindb.cn/cn/help/130/FunctionsandCommands/CommandsReferences/l/login.html?highlight=login
	Login(l *LoginRequest) error

	// Logout dolphindb.
	// See DolphinDB function `logout`: https://www.dolphindb.cn/cn/help/130/FunctionsandCommands/CommandsReferences/l/logout.html?highlight=logout
	Logout() error
}

// Login dolphindb.
// See DolphinDB function `login`: https://www.dolphindb.cn/cn/help/130/FunctionsandCommands/CommandsReferences/l/login.html?highlight=login
func (c *dolphindb) Login(l *LoginRequest) error {
	err := dialer.Login(c, l.UserID, l.Password)
	c.SetPassword(l.Password)
	c.SetUserID(l.UserID)
	return err
}

// Logout dolphindb.
// See DolphinDB function `logout`: https://www.dolphindb.cn/cn/help/130/FunctionsandCommands/CommandsReferences/l/logout.html?highlight=logout
func (c *dolphindb) Logout() error {
	_, err := c.RunScript("logout()")

	return err
}
