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
	"github.com/daiguadaidai/dba-biejiao-program-code/09_cobra_demo/cobra-demo/config"
	"github.com/liudng/godump"
)

var dbConfig *config.DBConfig

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "cobra-demo",
	Short: "cobra 第一个示例",
	Long: `cobra 具体的描述在这里, 我是一个Root命令
./cobra-demo --help
`,
}

// rootCmd represents the base command when called without any subcommands
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "子命令",
	Long: `我是一个子命令
Exmaple:
./cobra-demo run --help

./cobra-demo run \
    --host=127.0.0.1 \
    --port=3306 \
    --username=HH \
    --password=oracle \
`,

	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		godump.Dump(dbConfig)
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
	dbConfig = new(config.DBConfig)

	rootCmd.AddCommand(runCmd)

	runCmd.PersistentFlags().StringVar(&dbConfig.Host, "host", "0.0.0.0",
		"这是一个IP")
	runCmd.PersistentFlags().IntVar(&dbConfig.Port, "port", 3333,
		"这是一个端口")
	runCmd.PersistentFlags().StringVar(&dbConfig.Username, "username", "username",
		"这是一个username")
	runCmd.PersistentFlags().StringVar(&dbConfig.Password, "password", "password",
		"这是一个password")
}

