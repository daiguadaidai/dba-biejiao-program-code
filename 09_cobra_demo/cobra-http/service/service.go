package service

import (
	"github.com/daiguadaidai/dba-biejiao-program-code/09_cobra_demo/cobra-http/config"
	"net/http"
	"fmt"
)

func Start(_httpServerConfig *config.HttpServerConfig) {
	http.HandleFunc("/", IndexHandler)

	fmt.Println("监听的地址为:", _httpServerConfig.Address())
	http.ListenAndServe(_httpServerConfig.Address(), nil)

}

func IndexHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "hello world")
}

