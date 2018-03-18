package main

import (
    "flag"
    "fmt"
)

func main() {
    var ip = flag.Int("f", 1234, "help message for flagname")

    flag.Parse()

    fmt.Println(*ip)
}
