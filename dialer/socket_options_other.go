//go:build !linux
// +build !linux

package dialer

import "net"

func setPlatformTCPSocketOptions(conn *net.TCPConn, opt tcpSocketOptions) error {
	return nil
}
