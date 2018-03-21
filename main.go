package main

import (
	"fmt"
	"os"

	"github.com/qiniuts/qlogctl/cmd"
)

func main() {
	qlogctl := cmd.BuildApp()
	err := qlogctl.Run(os.Args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
