package riffdbopenfga

import (
	"context"
	"errors"

	"github.com/google/uuid"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage"
	generated "github.com/riffdb/riffdb-openfga/generated/go"
)

const maximumAuthorizationModelBytes = 262144

func decodeAuthorizationModel(encoded []byte, expectedStore, expectedID, expectedSchema string) (*openfgav1.AuthorizationModel, error) {
	model := new(openfgav1.AuthorizationModel)
	if err := unmarshalCanonical(encoded, model, maximumAuthorizationModelBytes); err != nil {
		return nil, err
	}
	if model.GetId() != expectedID || model.GetSchemaVersion() != expectedSchema || expectedStore == "" {
		return nil, errors.New("riffdb-openfga: stored authorization model identity is inconsistent")
	}
	if len(model.GetTypeDefinitions()) == 0 {
		return nil, storage.ErrNotFound
	}
	return model, nil
}

func (datastore *Datastore) WriteAuthorizationModel(ctx context.Context, store string, model *openfgav1.AuthorizationModel) error {
	if model == nil || len(model.GetTypeDefinitions()) == 0 {
		return nil
	}
	encoded, err := marshalProto(model, maximumAuthorizationModelBytes)
	if err != nil {
		return err
	}
	result, err := datastore.client.WriteAuthorizationModel(ctx, generated.WriteAuthorizationModelInput{
		StoreId: store, ModelId: model.GetId(), SchemaVersion: model.GetSchemaVersion(), SerializedProtobuf: encoded, RequestId: uuid.NewString(),
	})
	if err != nil {
		return mapError(ctx, err)
	}
	switch result.Outcome.(type) {
	case generated.WriteAuthorizationModelModelWritten:
		return nil
	case generated.WriteAuthorizationModelModelAlreadyExists:
		return storage.ErrCollision
	default:
		return errors.New("riffdb-openfga: unknown WriteAuthorizationModel outcome")
	}
}

func (datastore *Datastore) ReadAuthorizationModel(ctx context.Context, store, id string) (*openfgav1.AuthorizationModel, error) {
	result, err := datastore.client.GetAuthorizationModel(ctx, generated.GetAuthorizationModelParams{StoreId: store, ModelId: id}, generated.QueryOptions{})
	if err != nil {
		return nil, mapError(ctx, err)
	}
	switch found := result.Value.(type) {
	case generated.GetAuthorizationModelFound:
		return decodeAuthorizationModel(found.Model.SerializedProtobuf, store, found.Model.ModelId, found.Model.SchemaVersion)
	case generated.GetAuthorizationModelNotFound:
		return nil, storage.ErrNotFound
	default:
		return nil, errors.New("riffdb-openfga: unknown GetAuthorizationModel outcome")
	}
}

func (datastore *Datastore) ReadAuthorizationModels(ctx context.Context, store string, options storage.ReadAuthorizationModelsOptions) ([]*openfgav1.AuthorizationModel, string, error) {
	pageSize := options.Pagination.PageSize
	if pageSize <= 0 {
		pageSize = storage.DefaultPageSize
	}
	afterValue := options.Pagination.From
	models := make([]*openfgav1.AuthorizationModel, 0, pageSize)
	for len(models) < pageSize {
		var after *string
		if afterValue != "" {
			after = &afterValue
		}
		result, err := datastore.client.ListAuthorizationModels(ctx, generated.ListAuthorizationModelsParams{StoreId: store, After: after}, generated.QueryOptions{})
		if err != nil {
			return nil, "", mapError(ctx, err)
		}
		found, ok := result.Value.(generated.ListAuthorizationModelsFound)
		if !ok {
			return nil, "", errors.New("riffdb-openfga: unknown ListAuthorizationModels outcome")
		}
		if len(found.Models) == 0 {
			return models, "", nil
		}
		row := found.Models[0]
		model, err := decodeAuthorizationModel(row.SerializedProtobuf, store, row.ModelId, row.SchemaVersion)
		if err != nil {
			return nil, "", err
		}
		models = append(models, model)
		afterValue = result.NextCursor
		if afterValue == "" {
			return models, "", nil
		}
	}
	return models, afterValue, nil
}

func (datastore *Datastore) FindLatestAuthorizationModel(ctx context.Context, store string) (*openfgav1.AuthorizationModel, error) {
	result, err := datastore.client.LatestAuthorizationModel(ctx, generated.LatestAuthorizationModelParams{StoreId: store}, generated.QueryOptions{})
	if err != nil {
		return nil, mapError(ctx, err)
	}
	found, ok := result.Value.(generated.LatestAuthorizationModelFound)
	if !ok || len(found.Models) == 0 {
		return nil, storage.ErrNotFound
	}
	row := found.Models[0]
	return decodeAuthorizationModel(row.SerializedProtobuf, store, row.ModelId, row.SchemaVersion)
}
