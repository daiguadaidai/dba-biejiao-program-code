package server

import (
	"github.com/daiguadaidai/dba-biejiao-program-code/10_blingbling/config"
	"net/http"
	"github.com/daiguadaidai/dba-biejiao-program-code/10_blingbling/server/handler"
	"fmt"
)

func Start(_reviewConfig *config.ReviewConfig, _httpServerConfig *config.HttpServerConfig) {
	config.SetReviewConfig(_reviewConfig)

	http.HandleFunc("/sql_review", handler.ReviewSqlHandler)

	fmt.Println("启动的地址: ", _httpServerConfig.Address())
	http.ListenAndServe(_httpServerConfig.Address(), nil)
}
