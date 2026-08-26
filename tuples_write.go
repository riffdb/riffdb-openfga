package riffdbopenfga

import (
	"context"
	"crypto/rand"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	generated "github.com/riffdb/riffdb-openfga/generated/go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	riffdb "riffdb.dev/application"
)

const maximumConditionContextBytes = 32768

var emptyConditionContextEncoding = []byte{0}

func encodeConditionContext(value *structpb.Struct) ([]byte, error) {
	encoded, err := marshalProto(value, maximumConditionContextBytes)
	if err != nil {
		return nil, err
	}
	if len(encoded) == 0 {
		return append([]byte(nil), emptyConditionContextEncoding...), nil
	}
	return encoded, nil
}

func decodeConditionContext(encoded []byte) (*structpb.Struct, error) {
	value := new(structpb.Struct)
	if len(encoded) == len(emptyConditionContextEncoding) && encoded[0] == emptyConditionContextEncoding[0] {
		return value, nil
	}
	if err := unmarshalCanonical(encoded, value, maximumConditionContextBytes); err != nil {
		return nil, err
	}
	return value, nil
}

type persistedTupleState struct {
	store, objectType, objectID, relation, user          string
	userObjectType, userObjectID, userRelation, userKind string
	conditionKey                                         string
	conditionContext                                     *[]byte
	insertedAt                                           riffdb.Instant
	active                                               bool
	revision                                             uint64
}

func instant(value time.Time) riffdb.Instant {
	return riffdb.Instant{Seconds: value.Unix(), Nanos: uint32(value.Nanosecond())}
}
func tupleIdentity(objectType, objectID, relation, user string) string {
	return objectType + "\x00" + objectID + "\x00" + relation + "\x00" + user
}

func tupleParts(store string, key tuple.TupleWithoutCondition) persistedTupleState {
	objectType, objectID := tuple.SplitObject(key.GetObject())
	userType, userID, userRelation := tuple.ToUserParts(key.GetUser())
	return persistedTupleState{store: store, objectType: objectType, objectID: objectID, relation: key.GetRelation(), user: key.GetUser(), userObjectType: userType, userObjectID: userID, userRelation: userRelation, userKind: map[bool]string{true: "userset", false: "user"}[tuple.GetUserTypeFromUser(key.GetUser()) == tuple.UserSet]}
}

func (datastore *Datastore) getTupleState(ctx context.Context, key persistedTupleState) (persistedTupleState, bool, error) {
	result, err := datastore.client.GetTupleState(ctx, generated.GetTupleStateParams{StoreId: key.store, ObjectType: key.objectType, ObjectId: key.objectID, Relation: key.relation, User: key.user}, generated.QueryOptions{})
	if err != nil {
		return persistedTupleState{}, false, mapError(ctx, err)
	}
	switch found := result.Value.(type) {
	case generated.GetTupleStateNotFound:
		return key, false, nil
	case generated.GetTupleStateFound:
		value := found.State
		return persistedTupleState{store: value.StoreId, objectType: value.ObjectType, objectID: value.ObjectId, relation: value.Relation, user: value.User, userObjectType: value.UserObjectType, userObjectID: value.UserObjectId, userRelation: value.UserRelation, userKind: value.UserKind, conditionKey: value.ConditionKey, conditionContext: value.ConditionContext, insertedAt: value.InsertedAt, active: value.Active, revision: value.Revision}, true, nil
	default:
		return persistedTupleState{}, false, errors.New("riffdb-openfga: unknown GetTupleState outcome")
	}
}

func generatedTupleState(value persistedTupleState) generated.TupleState {
	return generated.TupleState{StoreId: value.store, ObjectType: value.objectType, ObjectId: value.objectID, Relation: value.relation, User: value.user, UserObjectType: value.userObjectType, UserObjectId: value.userObjectID, UserRelation: value.userRelation, UserKind: value.userKind, ConditionKey: value.conditionKey, ConditionContext: value.conditionContext, InsertedAt: value.insertedAt, Active: value.active, Revision: value.revision}
}

func (datastore *Datastore) ensureTupleState(ctx context.Context, value persistedTupleState) error {
	result, err := datastore.client.EnsureTupleStates(ctx, generated.EnsureTupleStatesInput{States: []generated.TupleState{generatedTupleState(value)}, RequestId: uuid.NewString()})
	if err != nil {
		return mapError(ctx, err)
	}
	switch result.Outcome.(type) {
	case generated.EnsureTupleStatesTupleStatesEnsured, generated.EnsureTupleStatesTupleStateAlreadyExists:
		return nil
	default:
		return errors.New("riffdb-openfga: unknown EnsureTupleStates outcome")
	}
}

func encodeCondition(condition *openfgav1.RelationshipCondition) (string, *[]byte, error) {
	if condition == nil {
		return "", nil, nil
	}
	contextValue := condition.GetContext()
	if contextValue == nil {
		contextValue = new(structpb.Struct)
	}
	encoded, err := encodeConditionContext(contextValue)
	if err != nil {
		return "", nil, err
	}
	return condition.GetName(), &encoded, nil
}

func stateCondition(value persistedTupleState) (*openfgav1.RelationshipCondition, error) {
	if value.conditionKey == "" {
		return nil, nil
	}
	condition := &openfgav1.RelationshipCondition{Name: value.conditionKey}
	if value.conditionContext != nil {
		contextValue, err := decodeConditionContext(*value.conditionContext)
		if err != nil {
			return nil, err
		}
		condition.Context = contextValue
	}
	return condition, nil
}

func (datastore *Datastore) Write(ctx context.Context, store string, deletes storage.Deletes, writes storage.Writes, options ...storage.TupleWriteOption) error {
	if len(deletes)+len(writes) == 0 {
		return nil
	}
	if len(deletes)+len(writes) > MaxTuplesPerWrite {
		return errors.New("riffdb-openfga: tuple write exceeds MaxTuplesPerWrite")
	}
	writeOptions := storage.NewTupleWriteOptions(options...)
	now := time.Now().UTC()
	insertedAt := instant(now)
	partition, err := datastore.client.EnsureTuplePartition(ctx, generated.EnsureTuplePartitionInput{StoreId: store, RequestId: uuid.NewString()})
	if err != nil {
		return mapError(ctx, err)
	}
	switch partition.Outcome.(type) {
	case generated.EnsureTuplePartitionTuplePartitionEnsured, generated.EnsureTuplePartitionTuplePartitionAlreadyExists:
	default:
		return errors.New("riffdb-openfga: unknown EnsureTuplePartition outcome")
	}

	states := make(map[string]persistedTupleState, len(deletes)+len(writes))
	operations := make(map[string]generated.TupleOperation, len(deletes)+len(writes))
	load := func(parts persistedTupleState) (persistedTupleState, error) {
		identity := tupleIdentity(parts.objectType, parts.objectID, parts.relation, parts.user)
		if value, ok := states[identity]; ok {
			return value, nil
		}
		value, found, err := datastore.getTupleState(ctx, parts)
		if err != nil {
			return persistedTupleState{}, err
		}
		if !found {
			value = parts
			value.insertedAt = insertedAt
		}
		states[identity] = value
		return value, nil
	}
	for _, key := range deletes {
		if _, err := load(tupleParts(store, key)); err != nil {
			return err
		}
	}
	for _, key := range writes {
		if _, err := load(tupleParts(store, key)); err != nil {
			return err
		}
	}

	for _, key := range deletes {
		parts := tupleParts(store, key)
		identity := tupleIdentity(parts.objectType, parts.objectID, parts.relation, parts.user)
		value := states[identity]
		if !value.active {
			if writeOptions.OnMissingDelete == storage.OnMissingDeleteError {
				return storage.InvalidWriteInputError(key, openfgav1.TupleOperation_TUPLE_OPERATION_DELETE)
			}
			continue
		}
		value.active = false
		states[identity] = value
		operations[identity] = generated.TupleOperationDelete
	}
	for _, key := range writes {
		parts := tupleParts(store, key)
		identity := tupleIdentity(parts.objectType, parts.objectID, parts.relation, parts.user)
		value := states[identity]
		conditionKey, conditionContext, err := encodeCondition(key.GetCondition())
		if err != nil {
			return err
		}
		if value.active {
			if writeOptions.OnDuplicateInsert == storage.OnDuplicateInsertError {
				return storage.InvalidWriteInputError(key, openfgav1.TupleOperation_TUPLE_OPERATION_WRITE)
			}
			storedCondition, err := stateCondition(value)
			if err != nil {
				return err
			}
			if !proto.Equal(storedCondition, key.GetCondition()) {
				return storage.TupleConditionConflictError(key)
			}
			continue
		}
		value.userObjectType, value.userObjectID, value.userRelation, value.userKind = parts.userObjectType, parts.userObjectID, parts.userRelation, parts.userKind
		value.conditionKey, value.conditionContext, value.insertedAt, value.active = conditionKey, conditionContext, insertedAt, true
		states[identity] = value
		operations[identity] = generated.TupleOperationWrite
	}

	// Create only the still-absent rows. Dormant rows are not visible to any
	// OpenFGA read and make the following multi-row mutation fully atomic.
	for identity, desired := range states {
		current, found, err := datastore.getTupleState(ctx, desired)
		if err != nil {
			return err
		}
		if !found {
			dormant := desired
			dormant.active = false
			dormant.revision = 0
			dormant.conditionKey = ""
			dormant.conditionContext = nil
			if err := datastore.ensureTupleState(ctx, dormant); err != nil {
				return err
			}
			current, found, err = datastore.getTupleState(ctx, desired)
			if err != nil {
				return err
			}
			if !found {
				return storage.ErrTransactionalWriteFailed
			}
		}
		desired.revision = current.revision
		states[identity] = desired
	}

	entropy := ulid.Monotonic(rand.Reader, 0)
	mutations := make([]generated.TupleMutationInput, 0, len(deletes)+len(writes))
	added := make(map[string]struct{}, len(states))
	appendMutation := func(parts persistedTupleState, operation generated.TupleOperation, nextActive bool, conditionKey string, conditionContext *[]byte) error {
		identity := tupleIdentity(parts.objectType, parts.objectID, parts.relation, parts.user)
		if _, ok := added[identity]; ok {
			return nil
		}
		current, found, err := datastore.getTupleState(ctx, parts)
		if err != nil {
			return err
		}
		if !found {
			return storage.ErrTransactionalWriteFailed
		}
		changeID, err := ulid.New(ulid.Timestamp(now), entropy)
		if err != nil {
			return errors.New("riffdb-openfga: failed to generate changelog ULID")
		}
		mutations = append(mutations, generated.TupleMutationInput{StoreId: store, ChangeId: changeID.String(), ObjectType: parts.objectType, ObjectId: parts.objectID, Relation: parts.relation, User: parts.user, UserObjectType: parts.userObjectType, UserObjectId: parts.userObjectID, UserRelation: parts.userRelation, UserKind: parts.userKind, Operation: operation, ConditionKey: conditionKey, ConditionContext: conditionContext, InsertedAt: insertedAt, ExpectedActive: current.active, NextActive: nextActive, ExpectedRevision: current.revision})
		added[identity] = struct{}{}
		return nil
	}
	for _, key := range deletes {
		parts := tupleParts(store, key)
		identity := tupleIdentity(parts.objectType, parts.objectID, parts.relation, parts.user)
		if operations[identity] == generated.TupleOperationDelete {
			if err := appendMutation(parts, generated.TupleOperationDelete, false, "", nil); err != nil {
				return err
			}
		}
	}
	for _, key := range writes {
		parts := tupleParts(store, key)
		identity := tupleIdentity(parts.objectType, parts.objectID, parts.relation, parts.user)
		desired := states[identity]
		if operations[identity] == generated.TupleOperationWrite {
			if err := appendMutation(parts, generated.TupleOperationWrite, true, desired.conditionKey, desired.conditionContext); err != nil {
				return err
			}
		}
	}
	if len(mutations) == 0 {
		return nil
	}
	result, err := datastore.client.ApplyTupleMutations(ctx, generated.ApplyTupleMutationsInput{Mutations: mutations, RequestId: uuid.NewString()})
	if err != nil {
		return mapError(ctx, err)
	}
	switch result.Outcome.(type) {
	case generated.ApplyTupleMutationsTupleMutationsApplied:
		return nil
	case generated.ApplyTupleMutationsTupleStateChanged, generated.ApplyTupleMutationsTupleStateMissing, generated.ApplyTupleMutationsTupleChangeAlreadyExists:
		return storage.ErrTransactionalWriteFailed
	default:
		return errors.New("riffdb-openfga: unknown ApplyTupleMutations outcome")
	}
}
