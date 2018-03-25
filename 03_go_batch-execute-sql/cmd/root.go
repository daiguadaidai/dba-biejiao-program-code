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

    "03_go_batch-execute-sql/imp"
    "03_go_batch-execute-sql/parser"

    "github.com/spf13/cobra"
)

// var cfgFile string
var argParer *parser.ArgParser = new(parser.ArgParser)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
    Use:   "batch-execute-sql",
    Short: "批量执行sql语句",
    Long: `
批量执行sql语句
    1.通过指定多个ip,
    2.通过指定一个ip文件

例子
    batch-execute-sql --host-port="127.0.0.1:3307" --sql="SHOW MASTER STATUS"
    batch-execute-sql --host-port="127.0.0.1:3307" --host-port="127.0.0.1:3307" --sql="SHOW MASTER STATUS"
    batch-execute-sql --file="/tmp/ip.txt" --sql="SHOW MASTER STATUS"
`,
    // Uncomment the following line if your bare application
    // has an action associated with it:
    Run: func(cmd *cobra.Command, args []string) {
        group_type, err := argParer.Parse()
        if err != nil {
            fmt.Printf("输入的参数有问题. %v\n", err)
            cmd.Help()
            return
        }

        imp.Start(argParer, group_type)
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
    // cobra.OnInitialize(initConfig)

    // Here you will define your flags and configuration settings.
    // Cobra supports persistent flags, which, if defined here,
    // will be global for your application.
    // rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.batchexecutesql.yaml)")

    // Cobra also supports local flags, which will only run
    // when this action is called directly.
    // rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
    rootCmd.Flags().StringArrayVarP(&argParer.Hosts, "host-port", "a", nil, "IP列表")
    rootCmd.Flags().StringVarP(&argParer.File, "file", "f", "",
        "指定ip文件文件中 格式: host:port, 每一行一个ip")
    rootCmd.Flags().StringVarP(&argParer.Sql, "sql", "s", "", "需要执行的SQL")
}

// initConfig reads in config file and ENV variables if set.
/*
func initConfig() {
    if cfgFile != "" {
        // Use config file from the flag.
        viper.SetConfigFile(cfgFile)
    } else {
        // Find home directory.
        home, err := homedir.Dir()
        if err != nil {
            fmt.Println(err)
            os.Exit(1)
        }

        // Search config in home directory with name ".batchexecutesql" (without extension).
        viper.AddConfigPath(home)
        viper.SetConfigName(".batchexecutesql")
    }

    viper.AutomaticEnv() // read in environment variables that match

    // If a config file is found, read it in.
    if err := viper.ReadInConfig(); err == nil {
        fmt.Println("Using config file:", viper.ConfigFileUsed())
    }
}
*/
