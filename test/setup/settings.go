package setup

import (
	"math/rand"
	"strconv"
)

func getPort(ports []int) (int, []int) {
	randomIndex := rand.Intn(len(ports))
	return ports[randomIndex], append(ports[:randomIndex], ports[randomIndex+1:]...)
}

var ports = []int{20902, 20903, 20904, 20905}
var IP = "127.0.0.1"
var Port, remainPorts = getPort(ports)
var CtlPort = 20900

var (
	UserName        = "admin"
	Password        = "123456"
	Address         = IP + ":" + strconv.Itoa(Port)
	Address2        = IP + ":" + strconv.Itoa(remainPorts[0])
	Address3        = IP + ":" + strconv.Itoa(remainPorts[1])
	Address4        = IP + ":" + strconv.Itoa(remainPorts[2])
	CtlAdress       = IP + ":" + strconv.Itoa(CtlPort)
	LocalIP         = "127.0.0.1"
	SubPort         = 13456
	Reverse_subPort = 0
	WORK_DIR        = "/home/codes/api-go/data"
	DATA_DIR        = "/home/codes/api-go/data"
)

var HA_sites = []string{Address, Address2, Address3, Address4}
