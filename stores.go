package riffdbopenfga

import (
	"context"
	"errors"

	"github.com/google/uuid"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage"
	generated "github.com/riffdb/riffdb-openfga/generated/go"
	riffdb "riffdb.dev/application"
)

func storeProto(id, name string, createdAt, updatedAt riffdb.Instant) *openfgav1.Store {
	return &openfgav1.Store{
		Id: id, Name: name,
		CreatedAt: protoTimestamp(createdAt), UpdatedAt: protoTimestamp(updatedAt),
	}
}

func (datastore *Datastore) getStoreWithRevision(ctx context.Context, id string) (*openfgav1.Store, uint64, error) {
	result, err := datastore.client.GetStore(ctx, generated.GetStoreParams{RegistryId: registryID, StoreId: id}, generated.QueryOptions{})
	if err != nil {
		return nil, 0, mapError(ctx, err)
	}
	switch found := result.Value.(type) {
	case generated.GetStoreFound:
		return storeProto(found.Store.StoreId, found.Store.Name, found.Store.CreatedAt, found.Store.UpdatedAt), found.Store.Revision, nil
	case generated.GetStoreNotFound:
		return nil, 0, storage.ErrNotFound
	default:
		return nil, 0, errors.New("riffdb-openfga: unknown GetStore outcome")
	}
}

func (datastore *Datastore) CreateStore(ctx context.Context, store *openfgav1.Store) (*openfgav1.Store, error) {
	if store == nil || store.GetId() == "" || store.GetName() == "" {
		return nil, errors.New("riffdb-openfga: store id and name are required")
	}
	result, err := datastore.commandClient().CreateStore(ctx, generated.CreateStoreInput{
		RegistryId: registryID, StoreId: store.GetId(), Name: store.GetName(), RequestId: uuid.NewString(),
	})
	if err != nil {
		return nil, mapError(ctx, err)
	}
	switch outcome := result.Outcome.(type) {
	case generated.CreateStoreStoreCreated:
		return storeProto(outcome.Store.StoreId, outcome.Store.Name, outcome.Store.CreatedAt, outcome.Store.UpdatedAt), nil
	case generated.CreateStoreStoreAlreadyExists:
		return nil, storage.ErrCollision
	default:
		return nil, errors.New("riffdb-openfga: unknown CreateStore outcome")
	}
}

func (datastore *Datastore) DeleteStore(ctx context.Context, id string) error {
	for attempt := uint32(0); attempt < datastore.commandAttempts; attempt++ {
		_, revision, err := datastore.getStoreWithRevision(ctx, id)
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		result, err := datastore.commandClient().DeleteStore(ctx, generated.DeleteStoreInput{
			RegistryId: registryID, StoreId: id, ExpectedRevision: revision, RequestId: uuid.NewString(),
		})
		if err != nil {
			return mapError(ctx, err)
		}
		switch result.Outcome.(type) {
		case generated.DeleteStoreStoreDeleted, generated.DeleteStoreStoreMissing:
			return nil
		case generated.DeleteStoreStoreChanged:
			continue
		default:
			return errors.New("riffdb-openfga: unknown DeleteStore outcome")
		}
	}
	return storage.ErrTransactionalWriteFailed
}

func (datastore *Datastore) GetStore(ctx context.Context, id string) (*openfgav1.Store, error) {
	store, _, err := datastore.getStoreWithRevision(ctx, id)
	return store, err
}

// maxStoreQueryPage is the compiler-declared `Limit<50>` maximum for the store
// listing queries. Store rows are small, so this matches OpenFGA's own default
// page rather than any cost ceiling.
const maxStoreQueryPage = 50

func (datastore *Datastore) ListStores(ctx context.Context, options storage.ListStoresOptions) ([]*openfgav1.Store, string, error) {
	pageSize := options.Pagination.PageSize
	if pageSize <= 0 {
		pageSize = storage.DefaultPageSize
	}
	if pageSize > 500 {
		pageSize = 500
	}
	// A caller may change page size while continuing a listing. That used to
	// force a fixed one-row internal page, because a cursor bound every query
	// input including the take limit. SPEC OQ-068 excludes the submitted page
	// cardinality from an ordinary ordered binding's cursor hash, so the page
	// can now be sized to the shortfall and the cursor still continues.
	afterValue := options.Pagination.From
	type storeRow struct {
		id, name             string
		createdAt, updatedAt riffdb.Instant
	}
	rows := make([]storeRow, 0, pageSize)
	appendRows := func(values []struct {
		StoreId   string
		Name      string
		CreatedAt riffdb.Instant
		UpdatedAt riffdb.Instant
		Revision  uint64
	}) {
		for _, value := range values {
			rows = append(rows, storeRow{id: value.StoreId, name: value.Name, createdAt: value.CreatedAt, updatedAt: value.UpdatedAt})
		}
	}
	for len(rows) < pageSize {
		var after *string
		if afterValue != "" {
			after = &afterValue
		}
		// Ask for exactly the shortfall: a cursor continues from the end of
		// the page it arrived with, so a kept-in-part page loses its remainder.
		want := pageSize - len(rows)
		if want > maxStoreQueryPage {
			want = maxStoreQueryPage
		}
		limit := uint32(want)
		previousLength := len(rows)
		switch {
		case len(options.IDs) > 0 && options.Name != "":
			result, err := datastore.client.ListStoresByIdsAndName(ctx, generated.ListStoresByIdsAndNameParams{RegistryId: registryID, Ids: options.IDs, Name: options.Name, Limit: &limit, After: after}, generated.QueryOptions{})
			if err != nil {
				return nil, "", mapError(ctx, err)
			}
			found, ok := result.Value.(generated.ListStoresByIdsAndNameFound)
			if !ok {
				return nil, "", errors.New("riffdb-openfga: unknown ListStores outcome")
			}
			appendRows(found.Stores)
			afterValue = result.NextCursor
		case len(options.IDs) > 0:
			result, err := datastore.client.ListStoresByIds(ctx, generated.ListStoresByIdsParams{RegistryId: registryID, Ids: options.IDs, Limit: &limit, After: after}, generated.QueryOptions{})
			if err != nil {
				return nil, "", mapError(ctx, err)
			}
			found, ok := result.Value.(generated.ListStoresByIdsFound)
			if !ok {
				return nil, "", errors.New("riffdb-openfga: unknown ListStores outcome")
			}
			appendRows(found.Stores)
			afterValue = result.NextCursor
		case options.Name != "":
			result, err := datastore.client.ListStoresByName(ctx, generated.ListStoresByNameParams{RegistryId: registryID, Name: options.Name, Limit: &limit, After: after}, generated.QueryOptions{})
			if err != nil {
				return nil, "", mapError(ctx, err)
			}
			found, ok := result.Value.(generated.ListStoresByNameFound)
			if !ok {
				return nil, "", errors.New("riffdb-openfga: unknown ListStores outcome")
			}
			appendRows(found.Stores)
			afterValue = result.NextCursor
		default:
			result, err := datastore.client.ListStoresAll(ctx, generated.ListStoresAllParams{RegistryId: registryID, Limit: &limit, After: after}, generated.QueryOptions{})
			if err != nil {
				return nil, "", mapError(ctx, err)
			}
			found, ok := result.Value.(generated.ListStoresAllFound)
			if !ok {
				return nil, "", errors.New("riffdb-openfga: unknown ListStores outcome")
			}
			appendRows(found.Stores)
			afterValue = result.NextCursor
		}
		if len(rows) == previousLength || afterValue == "" {
			break
		}
	}
	stores := make([]*openfgav1.Store, 0, len(rows))
	for _, row := range rows {
		stores = append(stores, storeProto(row.id, row.name, row.createdAt, row.updatedAt))
	}
	return stores, afterValue, nil
}
