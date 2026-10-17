// Package riffdbopenfga implements OpenFGA's datastore interface over a
// compiled RiffDB application.
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
	RiffDBDriverVersion = "v0.1.0-dev.19"

	// MaxTuplesPerWrite matches OpenFGA's default atomic write bound. The
	// compiled profile separately proves a bounded aggregate byte budget.
	MaxTuplesPerWrite = 100

	// MaxTypesPerAuthorizationModel matches the advertised OpenFGA datastore
	// default.
	MaxTypesPerAuthorizationModel = 100

	// DefaultPageSize matches OpenFGA's datastore default. Individual named
	// queries will also have explicit finite upper bounds.
	DefaultPageSize = 50
)

// Config controls a datastore backed by an authenticated RiffDB session.
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
