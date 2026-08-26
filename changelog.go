package riffdbopenfga

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	generated "github.com/riffdb/riffdb-openfga/generated/go"
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
			contextValue, err := decodeConditionContext(*encoded)
			if err != nil {
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

const (
	changeCursorVersion      = 1
	maximumChangePageSize    = 500
	maximumChangeCursorBytes = 22000
)

type changeCursorInstant struct {
	Seconds int64  `json:"s"`
	Nanos   uint32 `json:"n"`
}

func newChangeCursorInstant(value time.Time) changeCursorInstant {
	return changeCursorInstant{Seconds: value.Unix(), Nanos: uint32(value.Nanosecond())}
}

func (value changeCursorInstant) valid() bool {
	return value.Nanos < 1_000_000_000
}

func (value changeCursorInstant) riffDB() riffdb.Instant {
	return riffdb.Instant{Seconds: value.Seconds, Nanos: value.Nanos}
}

type changeCursor struct {
	Version       int                  `json:"v"`
	Cursor        string               `json:"c,omitempty"`
	Terminal      bool                 `json:"t,omitempty"`
	Store         string               `json:"s"`
	ObjectType    string               `json:"o,omitempty"`
	SortDesc      bool                 `json:"d,omitempty"`
	HorizonOffset int64                `json:"f"`
	Horizon       changeCursorInstant  `json:"h"`
	Start         *changeCursorInstant `json:"a,omitempty"`
}

func encodeChangeCursor(value changeCursor) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func decodeChangeCursor(value, store, objectType string, sortDesc bool, horizonOffset time.Duration) (changeCursor, error) {
	if len(value) > maximumChangeCursorBytes {
		return changeCursor{}, storage.ErrInvalidContinuationToken
	}
	encoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return changeCursor{}, storage.ErrInvalidContinuationToken
	}
	var cursor changeCursor
	if json.Unmarshal(encoded, &cursor) != nil ||
		cursor.Version != changeCursorVersion ||
		cursor.Store != store ||
		cursor.ObjectType != objectType ||
		cursor.SortDesc != sortDesc ||
		cursor.HorizonOffset != int64(horizonOffset) ||
		!cursor.Horizon.valid() ||
		(cursor.Start != nil && !cursor.Start.valid()) ||
		len(cursor.Cursor) > 16384 ||
		(cursor.Terminal && cursor.Cursor != "") {
		return changeCursor{}, storage.ErrInvalidContinuationToken
	}
	return cursor, nil
}

func (datastore *Datastore) ReadChanges(ctx context.Context, store string, filter storage.ReadChangesFilter, options storage.ReadChangesOptions) ([]*openfgav1.TupleChange, string, error) {
	if filter.HorizonOffset < 0 {
		return nil, "", storage.ErrInvalidStartTime
	}
	cursor := changeCursor{
		Version:       changeCursorVersion,
		Store:         store,
		ObjectType:    filter.ObjectType,
		SortDesc:      options.SortDesc,
		HorizonOffset: int64(filter.HorizonOffset),
		Horizon:       newChangeCursorInstant(time.Now().UTC().Add(-filter.HorizonOffset)),
	}
	if options.Pagination.From != "" {
		identifier, parseErr := ulid.ParseStrict(options.Pagination.From)
		if parseErr == nil {
			startTime := time.UnixMilli(int64(identifier.Time())).UTC()
			if options.SortDesc {
				if startTime.Before(time.Unix(cursor.Horizon.Seconds, int64(cursor.Horizon.Nanos))) {
					cursor.Horizon = newChangeCursorInstant(startTime)
				}
			} else {
				start := newChangeCursorInstant(startTime)
				cursor.Start = &start
			}
		} else {
			var err error
			cursor, err = decodeChangeCursor(options.Pagination.From, store, filter.ObjectType, options.SortDesc, filter.HorizonOffset)
			if err != nil {
				return nil, "", err
			}
			if cursor.Terminal {
				return nil, "", storage.ErrNotFound
			}
		}
	}
	pageSize := options.Pagination.PageSize
	if pageSize <= 0 {
		pageSize = storage.DefaultPageSize
	}
	if pageSize > maximumChangePageSize {
		pageSize = maximumChangePageSize
	}
	changes := make([]*openfgav1.TupleChange, 0, pageSize)
	for len(changes) < pageSize {
		var after *string
		if cursor.Cursor != "" {
			after = &cursor.Cursor
		}
		var value any
		var next string
		horizon := cursor.Horizon.riffDB()
		var start *riffdb.Instant
		if cursor.Start != nil {
			value := cursor.Start.riffDB()
			start = &value
		}
		switch {
		case filter.ObjectType != "" && options.SortDesc:
			result, queryErr := datastore.client.ReadChangesByObjectDesc(ctx, generated.ReadChangesByObjectDescParams{StoreId: store, ObjectType: filter.ObjectType, Horizon: horizon, After: after}, generated.QueryOptions{})
			if queryErr != nil {
				return nil, "", mapError(ctx, queryErr)
			}
			value = result.Value
			next = result.NextCursor
		case filter.ObjectType != "":
			result, queryErr := datastore.client.ReadChangesByObjectAsc(ctx, generated.ReadChangesByObjectAscParams{StoreId: store, ObjectType: filter.ObjectType, Start: start, Horizon: horizon, After: after}, generated.QueryOptions{})
			if queryErr != nil {
				return nil, "", mapError(ctx, queryErr)
			}
			value = result.Value
			next = result.NextCursor
		case options.SortDesc:
			result, queryErr := datastore.client.ReadChangesDesc(ctx, generated.ReadChangesDescParams{StoreId: store, Horizon: horizon, After: after}, generated.QueryOptions{})
			if queryErr != nil {
				return nil, "", mapError(ctx, queryErr)
			}
			value = result.Value
			next = result.NextCursor
		default:
			result, queryErr := datastore.client.ReadChangesAsc(ctx, generated.ReadChangesAscParams{StoreId: store, Start: start, Horizon: horizon, After: after}, generated.QueryOptions{})
			if queryErr != nil {
				return nil, "", mapError(ctx, queryErr)
			}
			value = result.Value
			next = result.NextCursor
		}
		change, _, err := projectedChange(value)
		if err != nil {
			return nil, "", err
		}
		if change == nil {
			break
		}
		changes = append(changes, change)
		cursor.Cursor = next
		cursor.Terminal = next == ""
		if cursor.Terminal {
			break
		}
	}
	if len(changes) == 0 {
		return nil, "", storage.ErrNotFound
	}
	next, err := encodeChangeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	return changes, next, nil
}
