package service

import (
	"fmt"
	"github.com/spf13/viper"
)

func Start() {
	fmt.Println("service 里面:", viper.AllKeys())

	fmt.Println(viper.Get("my.servers"))
}
