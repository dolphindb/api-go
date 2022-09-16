package api

import "fmt"

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
	_, err := c.RunScript(fmt.Sprintf("login('%s','%s')", l.UserID, l.Password))

	return err
}

// Logout dolphindb.
// See DolphinDB function `logout`: https://www.dolphindb.cn/cn/help/130/FunctionsandCommands/CommandsReferences/l/logout.html?highlight=logout
func (c *dolphindb) Logout() error {
	_, err := c.RunScript("logout()")

	return err
}
