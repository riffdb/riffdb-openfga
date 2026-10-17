package riffdbopenfga

import (
	"context"
	"errors"

	"github.com/google/uuid"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	generated "github.com/riffdb/riffdb-openfga/generated/go"
)

const maximumAssertionBytes = 64000

func (datastore *Datastore) ReadAssertions(ctx context.Context, store, modelID string) ([]*openfgav1.Assertion, error) {
	result, err := datastore.client.GetAssertionSet(ctx, generated.GetAssertionSetParams{StoreId: store, ModelId: modelID}, generated.QueryOptions{})
	if err != nil {
		return nil, mapError(ctx, err)
	}
	switch found := result.Value.(type) {
	case generated.GetAssertionSetNotFound:
		return []*openfgav1.Assertion{}, nil
	case generated.GetAssertionSetFound:
		response := new(openfgav1.ReadAssertionsResponse)
		if err := unmarshalCanonical(found.Assertions.SerializedProtobuf, response, maximumAssertionBytes); err != nil {
			return nil, err
		}
		if response.GetAuthorizationModelId() != modelID {
			return nil, errors.New("riffdb-openfga: stored assertion identity is inconsistent")
		}
		return response.GetAssertions(), nil
	default:
		return nil, errors.New("riffdb-openfga: unknown GetAssertionSet outcome")
	}
}

func (datastore *Datastore) WriteAssertions(ctx context.Context, store, modelID string, assertions []*openfgav1.Assertion) error {
	response := &openfgav1.ReadAssertionsResponse{AuthorizationModelId: modelID, Assertions: assertions}
	encoded, err := marshalProto(response, maximumAssertionBytes)
	if err != nil {
		return err
	}
	for attempt := uint32(0); attempt < datastore.commandAttempts; attempt++ {
		current, err := datastore.client.GetAssertionSet(ctx, generated.GetAssertionSetParams{StoreId: store, ModelId: modelID}, generated.QueryOptions{})
		if err != nil {
			return mapError(ctx, err)
		}
		switch found := current.Value.(type) {
		case generated.GetAssertionSetNotFound:
			created, err := datastore.commandClient().CreateAssertionSet(ctx, generated.CreateAssertionSetInput{StoreId: store, ModelId: modelID, SerializedProtobuf: encoded, RequestId: uuid.NewString()})
			if err != nil {
				return mapError(ctx, err)
			}
			switch created.Outcome.(type) {
			case generated.CreateAssertionSetAssertionSetCreated:
				return nil
			case generated.CreateAssertionSetAssertionSetAlreadyExists:
				continue
			default:
				return errors.New("riffdb-openfga: unknown CreateAssertionSet outcome")
			}
		case generated.GetAssertionSetFound:
			updated, err := datastore.commandClient().UpdateAssertionSet(ctx, generated.UpdateAssertionSetInput{StoreId: store, ModelId: modelID, ExpectedRevision: found.Assertions.Revision, SerializedProtobuf: encoded, RequestId: uuid.NewString()})
			if err != nil {
				return mapError(ctx, err)
			}
			switch updated.Outcome.(type) {
			case generated.UpdateAssertionSetAssertionSetUpdated:
				return nil
			case generated.UpdateAssertionSetAssertionSetChanged, generated.UpdateAssertionSetAssertionSetMissing:
				continue
			default:
				return errors.New("riffdb-openfga: unknown UpdateAssertionSet outcome")
			}
		default:
			return errors.New("riffdb-openfga: unknown GetAssertionSet outcome")
		}
	}
	return errors.New("riffdb-openfga: assertion write conflicted repeatedly")
}
