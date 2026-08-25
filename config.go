// Package riffdbopenfga implements OpenFGA's datastore interface with a
// compiled RiffDB application.
//
// The repository is currently in bootstrap: configuration validation is
// available, but no incomplete datastore implementation is exported.
package riffdbopenfga

import (
	"errors"

	generated "github.com/riffdb/riffdb-openfga/generated/go"
	riffdb "riffdb.dev/application"
)

const (
	// OpenFGAVersion is the exact upstream server version whose datastore
	// contract this module targets.
	OpenFGAVersion = "v1.18.3"

	// RiffDBDriverVersion is the immutable development driver publication used
	// to generate and verify this adapter.
	RiffDBDriverVersion = "v0.1.0-dev.14"

	// MaxTuplesPerWrite is the adapter's advertised atomic write bound. Eight
	// full-size condition contexts fit RiffDB's statically proven command-graph
	// ceiling; OpenFGA batches larger API writes using this value.
	MaxTuplesPerWrite = 8

	// MaxTypesPerAuthorizationModel matches the advertised OpenFGA datastore
	// default.
	MaxTypesPerAuthorizationModel = 100

	// DefaultPageSize matches OpenFGA's datastore default. Individual named
	// queries will also have explicit finite upper bounds.
	DefaultPageSize = 50
)

// Config is the input to the eventual datastore constructor. Keeping the
// caller-owned session explicit prevents the adapter from duplicating RiffDB
// transport, credential, retry, or application-identity behavior.
type Config struct {
	Session         *riffdb.Session
	CommandAttempts uint32
	CloseSession    bool
}

// Validate rejects configuration that cannot safely be used by the generated
// application facade.
func (config Config) Validate() error {
	if config.Session == nil {
		return errors.New("riffdb-openfga: session is required")
	}
	if config.CommandAttempts < 1 || config.CommandAttempts > 10 {
		return errors.New("riffdb-openfga: command attempts must be between 1 and 10")
	}
	return nil
}

// NewDatastore constructs the adapter over the caller's authenticated RiffDB
// application session. Close leaves that session open unless CloseSession is
// explicitly selected.
func NewDatastore(config Config) (*Datastore, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	client, err := generated.NewClient(config.Session, config.CommandAttempts)
	if err != nil {
		return nil, err
	}
	return &Datastore{
		client:          client,
		session:         config.Session,
		commandAttempts: config.CommandAttempts,
		closeSession:    config.CloseSession,
	}, nil
}
