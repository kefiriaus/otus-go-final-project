package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "stats-daemon: scaffold only; server is not implemented")
	os.Exit(1)
}
