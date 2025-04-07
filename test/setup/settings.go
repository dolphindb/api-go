package setup

import (
	"math/rand"
	"strconv"
	"time"
)

func getPort(ports []int) (int, []int) {
	rand.Seed(time.Now().UnixNano())
	randomIndex := rand.Intn(len(ports))
	return ports[randomIndex], append(ports[:randomIndex], ports[randomIndex+1:]...)
}

var ports = []int{13002, 13003, 13004, 13005}
var IP = "192.168.100.9"
var Port, remainPorts = getPort(ports)
var CtlPort = 13000

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
	WORK_DIR        = "/hdd/hdd5/yzou/api_go_testing/codes/api-go/data"
	DATA_DIR        = "/hdd/hdd5/yzou/api_go_testing/codes/api-go/data"
)

var HA_sites = []string{Address, Address2, Address3, Address4}
