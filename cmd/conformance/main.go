package main

import (
	"fmt"
	"os"
	"os/exec"
)

var loopbackEnvironment = [...]string{
	"RIFFDB_LOOPBACK_SOCKET",
	"RIFFDB_LOOPBACK_MANIFEST_HASH",
	"RIFFDB_LOOPBACK_CATALOG_HASH",
	"RIFFDB_LOOPBACK_DATABASE",
	"RIFFDB_LOOPBACK_ROLE",
	"RIFFDB_LOOPBACK_ROLE_HASH",
	"RIFFDB_LOOPBACK_REMOTE_HASH",
	"RIFFDB_LOOPBACK_LINEAGE",
	"RIFFDB_LOOPBACK_VERSION",
	"RIFFDB_LOOPBACK_BUNDLE_HASH",
}

func main() {
	if len(os.Args) != len(loopbackEnvironment)+1 {
		fmt.Fprintf(os.Stderr, "riffdb-openfga conformance runner: got %d identity arguments, want %d\n", len(os.Args)-1, len(loopbackEnvironment))
		os.Exit(2)
	}

	environment := os.Environ()
	for index, name := range loopbackEnvironment {
		environment = append(environment, name+"="+os.Args[index+1])
	}

	testPattern := os.Getenv("RIFFDB_OPENFGA_TEST_RUN")
	if testPattern == "" {
		testPattern = "^TestLoopbackConformance$"
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
