package riffdbopenfga

import (
	"context"
	"errors"
	"sync"

	"github.com/openfga/openfga/pkg/storage"
	generated "github.com/riffdb/riffdb-openfga/generated/go"
	riffdb "riffdb.dev/application"
)

const registryID = "01900000-0000-7000-8000-000000000001"

// Datastore implements OpenFGA storage through a generated, compiled RiffDB
// application facade.
type Datastore struct {
	client          *generated.Client
	session         *riffdb.Session
	commandAttempts uint32
	closeSession    bool
	closeOnce       sync.Once
}

var _ storage.OpenFGADatastore = (*Datastore)(nil)

// MaxTuplesPerWrite returns the maximum combined delete/write count accepted
// by one atomic tuple command.
func (*Datastore) MaxTuplesPerWrite() int { return MaxTuplesPerWrite }

// MaxTypesPerAuthorizationModel returns the profile's model type limit.
func (*Datastore) MaxTypesPerAuthorizationModel() int { return MaxTypesPerAuthorizationModel }

// Close releases adapter-owned resources.
func (datastore *Datastore) Close() {
	datastore.closeOnce.Do(func() {
		if datastore.closeSession {
			datastore.session.Close()
		}
	})
}

func (datastore *Datastore) commandClient() *generated.Client {
	return datastore.client.WithReadAfterCommit(nil)
}

// IsReady proves that the compiled query path can reach RiffDB.
func (datastore *Datastore) IsReady(ctx context.Context) (storage.ReadinessStatus, error) {
	_, err := datastore.client.GetStore(ctx, generated.GetStoreParams{
		RegistryId: registryID,
		StoreId:    "00000000000000000000000000",
	}, generated.QueryOptions{})
	if err != nil {
		return storage.ReadinessStatus{Message: "RiffDB application query failed"}, mapError(ctx, err)
	}
	return storage.ReadinessStatus{IsReady: true}, nil
}

func mapError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	var applicationErr *riffdb.ApplicationError
	if errors.As(err, &applicationErr) {
		switch applicationErr.Details.Category {
		case "conflict":
			return storage.ErrTransactionalWriteFailed
		case "unavailable", "overloaded":
			return err
		}
	}
	return err
}
