package config

import "fmt"

const (
	LISTEN_HOST = "0.0.0.0"
	LISTEN_PORT = 18080
)

type HttpServerConfig struct {
	ListenHost string
	ListenPort int
}

func (this *HttpServerConfig) Address() string {
	return fmt.Sprintf("%v:%v", this.ListenHost, this.ListenPort)
}
