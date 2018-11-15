// Copyright © 2018 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/daiguadaidai/dba-biejiao-program-code/10_blingbling/config"
	"github.com/daiguadaidai/dba-biejiao-program-code/10_blingbling/server"
)

var reviewConfig *config.ReviewConfig
var httpServerConfig *config.HttpServerConfig

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "10_blingbling",
	Short: "blingbling简易开发",
	Long: `blingbling简易开发
./10_blingbling
`,
	Run: func(cmd *cobra.Command, args []string) {
		server.Start(reviewConfig, httpServerConfig)
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	reviewConfig = new(config.ReviewConfig)

	rootCmd.PersistentFlags().StringVar(&reviewConfig.RuleNameReg, "rule-name-reg",
		config.RULE_NAME_REG, "普通名称规则")
	rootCmd.PersistentFlags().StringVar(&reviewConfig.RuleIndexNameReg, "rule-index-name-reg",
		config.RULE_INDEX_NAME_REG, "索引名称规则")
	rootCmd.PersistentFlags().BoolVar(&reviewConfig.RuleAllowColumnNull, "rule-allow-column-null",
		config.RULE_ALLOW_COLUMN_NULL, "是否允许此段为空")
	rootCmd.PersistentFlags().BoolVar(&reviewConfig.RuleAllowDuplicateIndex, "rule-allow-duplicate-index",
		config.RULE_ALLOW_DUPLICATE_INDEX, "是否允许有重复索引")
	rootCmd.PersistentFlags().BoolVar(&reviewConfig.RuleAllowDeleteNoWhere, "rule-allow-delete-no-where",
		config.RULE_ALLOW_DELETE_NO_WHERE, "是否允许delete语句没有where子句")
	rootCmd.PersistentFlags().IntVar(&reviewConfig.RuleAllowDeleteMaxRows, "rule-allow-delete-max-rows",
		config.RULE_ALLOW_DELETE_MAX_ROWS, "delete允许删除的最大行数")

	httpServerConfig = new(config.HttpServerConfig)
	rootCmd.PersistentFlags().StringVar(&httpServerConfig.ListenHost, "listen-host",
		config.LISTEN_HOST, "HTTP服务ip")
	rootCmd.PersistentFlags().IntVar(&httpServerConfig.ListenPort, "listen-port",
		config.LISTEN_PORT, "HTTP服务端口")
}
