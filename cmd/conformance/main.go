package main

import (
	"fmt"
	"os"
	"os/exec"

	riffdbopenfga "github.com/riffdb/riffdb-openfga"
)

func main() {
	environment := os.Environ()
	if len(os.Args) == len(riffdbopenfga.LoopbackEnvironmentVariables)+1 {
		for index, name := range riffdbopenfga.LoopbackEnvironmentVariables {
			environment = append(environment, name+"="+os.Args[index+1])
		}
	} else if len(os.Args) != 1 || (os.Getenv("RIFFDB_LOOPBACK_SOCKET") == "" && os.Getenv("RIFFDB_DRIVER_SOCKET") == "") {
		fmt.Fprintf(os.Stderr, "riffdb-openfga conformance runner: got %d identity arguments, want %d (or RIFFDB_DRIVER_SOCKET in env)\n", len(os.Args)-1, len(riffdbopenfga.LoopbackEnvironmentVariables))
		os.Exit(2)
	}

	testPattern := os.Getenv("RIFFDB_OPENFGA_TEST_RUN")
	if testPattern == "" {
		testPattern = "^TestLoopbackConformance"
	}
	command := exec.Command("go", "test", "-v", "-run", testPattern, "-count=1", "-timeout=20m", ".")
	command.Env = environment
	command.Stdin = os.Stdin
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			os.Exit(exitError.ExitCode())
		}
		fmt.Fprintf(os.Stderr, "riffdb-openfga conformance runner: %v\n", err)
		os.Exit(1)
	}
}
