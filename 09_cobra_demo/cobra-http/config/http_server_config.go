package config

import "fmt"

type HttpServerConfig struct {
	Host string
	Port int
}

func (this *HttpServerConfig) Address() string {
	return fmt.Sprintf("%v:%v", this.Host, this.Port)
}
