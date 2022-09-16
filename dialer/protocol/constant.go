package protocol

const (
	// NewLine is the byte format of \n.
	NewLine byte = '\n'
	// EmptySpace is the bytes format of space.
	EmptySpace byte = ' '
	// StringSep is the bytes format of string sep.
	StringSep byte = 0
)

var (
	// APIBytes is the bytes format of API.
	APIBytes = []byte("API")
	// RespOK is the bytes format of OK.
	RespOK = []byte("OK")
)
