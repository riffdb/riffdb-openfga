package main

import (
	"os"

	"github.com/riffdb/riffdb-openfga/internal/openfgarun"
)

func main() {
	rootCmd := openfgarun.NewRootCommand()
	if args, ok := openfgarun.ApplyLoopbackArgs(os.Args[1:]); ok {
		rootCmd.SetArgs(args)
	}
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
