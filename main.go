package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"

	"github.com/USA-RedDragon/sentinel_tunnel/internal/sentinel_connection"
)

// https://goreleaser.com/cookbooks/using-main.version/
//
//nolint:golint,gochecknoglobals
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"

	infoLog = log.New(os.Stdout,
		"INFO: ",
		log.Ldate|log.Ltime)
	errorLog = log.New(os.Stderr,
		"ERROR: ",
		log.Ldate|log.Ltime)
	fatalLog = log.New(os.Stderr,
		"FATAL: ",
		log.Ldate|log.Ltime)
)

type SentinelTunnellingDbConfig struct {
	Name       string
	Local_port string
}

type SentinelTunnellingConfiguration struct {
	Sentinels_addresses_list []string
	Databases                []SentinelTunnellingDbConfig
}

type SentinelTunnellingClient struct {
	configuration       SentinelTunnellingConfiguration
	sentinel_connection *sentinel_connection.Sentinel_connection
}

type get_db_address_by_name_function func(db_name string) (string, error)

func NewSentinelTunnellingClient(config_file_location string) *SentinelTunnellingClient {
	data, err := os.ReadFile(config_file_location)
	if err != nil {
		fatalLog.Printf("an error has occur during configuration read: %v\n", err.Error())
		os.Exit(1)
	}

	Tunnelling_client := SentinelTunnellingClient{}
	err = json.Unmarshal(data, &(Tunnelling_client.configuration))
	if err != nil {
		fatalLog.Printf("an error has occur during configuration unmarshal: %v\n", err.Error())
		os.Exit(1)
	}

	Tunnelling_client.sentinel_connection, err =
		sentinel_connection.NewSentinelConnection(Tunnelling_client.configuration.Sentinels_addresses_list)
	if err != nil {
		fatalLog.Printf("an error has occur during sentinel connection creation: %v\n", err.Error())
		os.Exit(1)
	}

	infoLog.Println("done initializing tunnelling")

	return &Tunnelling_client
}

func createTunnelling(conn1 net.Conn, conn2 net.Conn) {
	io.Copy(conn1, conn2)
	conn1.Close()
	conn2.Close()
}

func handleConnection(c net.Conn, db_name string,
	get_db_address_by_name get_db_address_by_name_function) {
	db_address, err := get_db_address_by_name(db_name)
	if err != nil {
		errorLog.Printf("cannot get db address for %s: %v\n", db_name, err.Error())
		c.Close()
		return
	}
	db_conn, err := net.Dial("tcp", db_address)
	if err != nil {
		errorLog.Printf("cannot connect to db %s: %v\n", db_name, err.Error())
		c.Close()
		return
	}
	go createTunnelling(c, db_conn)
	go createTunnelling(db_conn, c)
}

func handleSigleDbConnections(listening_port string, db_name string,
	get_db_address_by_name get_db_address_by_name_function) {

	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", listening_port))
	if err != nil {
		fatalLog.Printf("cannot listen to port %s: %v\n", listening_port, err.Error())
			listening_port, err.Error())
	}

	infoLog.Printf("listening on port %s for connections to database: %s\n", listening_port, dbName)
	for {
		conn, err := listener.Accept()
		if err != nil {
			fatalLog.Printf("cannot accept connections on port %s: %v\n", listening_port, err.Error())
			os.Exit(1)
		}
		go handleConnection(conn, db_name, get_db_address_by_name)
	}

}

func (st_client *SentinelTunnellingClient) Start() {
	for _, db_conf := range st_client.configuration.Databases {
		go handleSigleDbConnections(db_conf.Local_port, db_conf.Name,
			st_client.sentinel_connection.GetAddressByDbName)
	}
}

func main() {
	infoLog.Printf("Redis Sentinel Tunnel %s (%s built %s)\n", version, string([]byte(commit)[:7]), date)
	if len(os.Args) < 2 {
		fatalLog.Printf("not enough arguments\n")
		fatalLog.Printf("usage: %s <config_file_path>\n", os.Args[0])
		return
	}
	st_client := NewSentinelTunnellingClient(os.Args[1])
	st_client.Start()
	for {
		time.Sleep(1000 * time.Millisecond)
	}
}
