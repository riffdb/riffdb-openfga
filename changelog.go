package riffdbopenfga

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	generated "github.com/riffdb/riffdb-openfga/generated/go"
	"google.golang.org/protobuf/types/known/structpb"
	riffdb "riffdb.dev/application"
)

func projectedChange(result any) (*openfgav1.TupleChange, string, error) {
	value := reflect.ValueOf(result)
	if value.Kind() == reflect.Pointer {
		value = value.Elem()
	}
	if !value.IsValid() || value.Kind() != reflect.Struct {
		return nil, "", errors.New("riffdb-openfga: invalid changelog query outcome")
	}
	rows := value.FieldByName("Changes")
	if !rows.IsValid() || rows.Kind() != reflect.Slice {
		return nil, "", errors.New("riffdb-openfga: invalid generated changelog projection")
	}
	if rows.Len() == 0 {
		return nil, "", nil
	}
	row := rows.Index(0)
	stringField := func(name string) (string, error) {
		field := row.FieldByName(name)
		if !field.IsValid() || field.Kind() != reflect.String {
			return "", errors.New("riffdb-openfga: invalid generated changelog projection")
		}
		return field.String(), nil
	}
	changeID, err := stringField("ChangeId")
	if err != nil {
		return nil, "", err
	}
	objectType, err := stringField("ObjectType")
	if err != nil {
		return nil, "", err
	}
	objectID, err := stringField("ObjectId")
	if err != nil {
		return nil, "", err
	}
	relation, err := stringField("Relation")
	if err != nil {
		return nil, "", err
	}
	user, err := stringField("User")
	if err != nil {
		return nil, "", err
	}
	conditionKey, err := stringField("ConditionKey")
	if err != nil {
		return nil, "", err
	}
	inserted, ok := row.FieldByName("InsertedAt").Interface().(riffdb.Instant)
	if !ok {
		return nil, "", errors.New("riffdb-openfga: invalid generated changelog timestamp")
	}
	operation, ok := row.FieldByName("Operation").Interface().(generated.TupleOperation)
	if !ok {
		return nil, "", errors.New("riffdb-openfga: invalid generated changelog operation")
	}
	key := &openfgav1.TupleKey{Object: tuple.BuildObject(objectType, objectID), Relation: relation, User: user}
	if conditionKey != "" {
		condition := &openfgav1.RelationshipCondition{Name: conditionKey}
		encoded, ok := row.FieldByName("ConditionContext").Interface().(*[]byte)
		if !ok {
			return nil, "", errors.New("riffdb-openfga: invalid generated changelog condition")
		}
		if encoded != nil {
			contextValue := new(structpb.Struct)
			if err := unmarshalCanonical(*encoded, contextValue, 524288); err != nil {
				return nil, "", err
			}
			condition.Context = contextValue
		}
		key.Condition = condition
	}
	protoOperation := openfgav1.TupleOperation_TUPLE_OPERATION_WRITE
	if operation == generated.TupleOperationDelete {
		protoOperation = openfgav1.TupleOperation_TUPLE_OPERATION_DELETE
	}
	return &openfgav1.TupleChange{TupleKey: key, Operation: protoOperation, Timestamp: protoTimestamp(inserted)}, changeID, nil
}

func horizonULID(value time.Time) (string, error) {
	entropy := bytes.Repeat([]byte{0xff}, 10)
	identifier, err := ulid.New(ulid.Timestamp(value), bytes.NewReader(entropy))
	if err != nil {
		return "", storage.ErrInvalidStartTime
	}
	return identifier.String(), nil
}

func (datastore *Datastore) ReadChanges(ctx context.Context, store string, filter storage.ReadChangesFilter, options storage.ReadChangesOptions) ([]*openfgav1.TupleChange, string, error) {
	if filter.HorizonOffset < 0 {
		return nil, "", storage.ErrInvalidStartTime
	}
	fromValue := options.Pagination.From
	if fromValue != "" {
		if _, err := ulid.ParseStrict(fromValue); err != nil {
			return nil, "", storage.ErrInvalidContinuationToken
		}
	}
	horizon, err := horizonULID(time.Now().UTC().Add(-filter.HorizonOffset))
	if err != nil {
		return nil, "", err
	}
	pageSize := options.Pagination.PageSize
	if pageSize <= 0 {
		pageSize = storage.DefaultPageSize
	}
	changes := make([]*openfgav1.TupleChange, 0, pageSize)
	for len(changes) < pageSize {
		var from *string
		if fromValue != "" {
			from = &fromValue
		}
		var value any
		switch {
		case filter.ObjectType != "" && options.SortDesc:
			result, queryErr := datastore.client.ReadChangesByObjectDesc(ctx, generated.ReadChangesByObjectDescParams{StoreId: store, ObjectType: filter.ObjectType, From: from, Horizon: horizon}, generated.QueryOptions{})
			if queryErr != nil {
				return nil, "", mapError(ctx, queryErr)
			}
			value = result.Value
		case filter.ObjectType != "":
			result, queryErr := datastore.client.ReadChangesByObjectAsc(ctx, generated.ReadChangesByObjectAscParams{StoreId: store, ObjectType: filter.ObjectType, From: from, Horizon: horizon}, generated.QueryOptions{})
			if queryErr != nil {
				return nil, "", mapError(ctx, queryErr)
			}
			value = result.Value
		case options.SortDesc:
			result, queryErr := datastore.client.ReadChangesDesc(ctx, generated.ReadChangesDescParams{StoreId: store, From: from, Horizon: horizon}, generated.QueryOptions{})
			if queryErr != nil {
				return nil, "", mapError(ctx, queryErr)
			}
			value = result.Value
		default:
			result, queryErr := datastore.client.ReadChangesAsc(ctx, generated.ReadChangesAscParams{StoreId: store, From: from, Horizon: horizon}, generated.QueryOptions{})
			if queryErr != nil {
				return nil, "", mapError(ctx, queryErr)
			}
			value = result.Value
		}
		change, next, err := projectedChange(value)
		if err != nil {
			return nil, "", err
		}
		if change == nil {
			break
		}
		changes = append(changes, change)
		fromValue = next
	}
	if len(changes) == 0 {
		return nil, "", storage.ErrNotFound
	}
	return changes, fromValue, nil
}
