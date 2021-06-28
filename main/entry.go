package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("benchmark/db/storage/forwarder expected")
	}

	switch os.Args[1] {
	case "benchmark":
		perf()
	case "db":

	}
}
