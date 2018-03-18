package main

import (
    "fmt"

    "github.com/spf13/pflag"
)

func main() {
    var ip *int = pflag.Int("flagname", 1234, "help message for flagname")
    var ip2 *int = pflag.IntP("filename2", "p", 1234, "help message for flagname")
    pflag.Parse()

    fmt.Println(*ip)
    fmt.Println(*ip2)
}
