package riffdbopenfga

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"sync"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	generated "github.com/riffdb/riffdb-openfga/generated/go"
	"google.golang.org/protobuf/types/known/structpb"
	riffdb "riffdb.dev/application"
)

type tupleFetch func(context.Context, *string) (*openfgav1.Tuple, string, error)

type lazyTupleIterator struct {
	mu       sync.Mutex
	fetchers []tupleFetch
	index    int
	cursor   string
	head     *openfgav1.Tuple
	stopped  bool
	ordered  bool
}

var _ storage.TupleIterator = (*lazyTupleIterator)(nil)

func (iterator *lazyTupleIterator) fetch(ctx context.Context) (*openfgav1.Tuple, error) {
	for iterator.index < len(iterator.fetchers) {
		var after *string
		if iterator.cursor != "" {
			after = &iterator.cursor
		}
		value, next, err := iterator.fetchers[iterator.index](ctx, after)
		if err != nil {
			return nil, err
		}
		if value != nil {
			iterator.cursor = next
			if next == "" {
				iterator.index++
				iterator.cursor = ""
			}
			return value, nil
		}
		iterator.index++
		iterator.cursor = ""
	}
	return nil, storage.ErrIteratorDone
}

func (iterator *lazyTupleIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	iterator.mu.Lock()
	defer iterator.mu.Unlock()
	if iterator.stopped {
		return nil, storage.ErrIteratorDone
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if iterator.head != nil {
		value := iterator.head
		iterator.head = nil
		return value, nil
	}
	return iterator.fetch(ctx)
}

func (iterator *lazyTupleIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	iterator.mu.Lock()
	defer iterator.mu.Unlock()
	if iterator.stopped {
		return nil, storage.ErrIteratorDone
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if iterator.head == nil {
		value, err := iterator.fetch(ctx)
		if err != nil {
			return nil, err
		}
		iterator.head = value
	}
	return iterator.head, nil
}

func (iterator *lazyTupleIterator) Stop() {
	iterator.mu.Lock()
	iterator.stopped = true
	iterator.head = nil
	iterator.mu.Unlock()
}
func (iterator *lazyTupleIterator) IsOrdered() bool { return iterator.ordered }

// Generated query projections are intentionally anonymous Go structs. This
// narrow projector validates their generated shape before converting them to
// OpenFGA values, keeping the rest of the adapter independent of that detail.
func projectedTuple(result any) (*openfgav1.Tuple, error) {
	value := reflect.ValueOf(result)
	if value.Kind() == reflect.Pointer {
		value = value.Elem()
	}
	if !value.IsValid() || value.Kind() != reflect.Struct {
		return nil, errors.New("riffdb-openfga: invalid tuple query outcome")
	}
	rows := value.FieldByName("Tuples")
	var row reflect.Value
	if rows.IsValid() {
		if rows.Kind() != reflect.Slice || rows.Len() == 0 {
			return nil, nil
		}
		row = rows.Index(0)
	} else {
		row = value.FieldByName("Tuple")
		if !row.IsValid() {
			return nil, nil
		}
	}
	stringField := func(name string) (string, error) {
		field := row.FieldByName(name)
		if !field.IsValid() || field.Kind() != reflect.String {
			return "", errors.New("riffdb-openfga: invalid generated tuple projection")
		}
		return field.String(), nil
	}
	objectType, err := stringField("ObjectType")
	if err != nil {
		return nil, err
	}
	objectID, err := stringField("ObjectId")
	if err != nil {
		return nil, err
	}
	relation, err := stringField("Relation")
	if err != nil {
		return nil, err
	}
	user, err := stringField("User")
	if err != nil {
		return nil, err
	}
	conditionKey, err := stringField("ConditionKey")
	if err != nil {
		return nil, err
	}
	inserted, ok := row.FieldByName("InsertedAt").Interface().(riffdb.Instant)
	if !ok {
		return nil, errors.New("riffdb-openfga: invalid generated tuple timestamp")
	}
	key := &openfgav1.TupleKey{Object: tuple.BuildObject(objectType, objectID), Relation: relation, User: user}
	if conditionKey != "" {
		contextField := row.FieldByName("ConditionContext")
		if !contextField.IsValid() {
			return nil, errors.New("riffdb-openfga: missing generated condition context")
		}
		encoded, ok := contextField.Interface().(*[]byte)
		if !ok {
			return nil, errors.New("riffdb-openfga: invalid generated condition context")
		}
		condition := &openfgav1.RelationshipCondition{Name: conditionKey}
		if encoded != nil {
			contextValue := new(structpb.Struct)
			if err := unmarshalCanonical(*encoded, contextValue, 524288); err != nil {
				return nil, err
			}
			condition.Context = contextValue
		}
		key.Condition = condition
	}
	return &openfgav1.Tuple{Key: key, Timestamp: protoTimestamp(inserted)}, nil
}

func conditionValues(values []string) []*string {
	if len(values) == 0 {
		return []*string{nil}
	}
	seen := make(map[string]struct{}, len(values))
	result := make([]*string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; !ok {
			copy := value
			result = append(result, &copy)
			seen[value] = struct{}{}
		}
	}
	return result
}

func (datastore *Datastore) tupleFetcher(store string, filter storage.ReadFilter, condition *string) (tupleFetch, error) {
	objectType, objectID := tuple.SplitObject(filter.Object)
	exactObject := objectType != "" && objectID != ""
	typeOnly := objectType != "" && objectID == ""
	if filter.Object != "" && !exactObject && !typeOnly {
		return nil, errors.New("riffdb-openfga: invalid tuple object filter")
	}
	return func(ctx context.Context, after *string) (*openfgav1.Tuple, string, error) {
		options := generated.QueryOptions{}
		finish := func(value any, cursor string, err error) (*openfgav1.Tuple, string, error) {
			if err != nil {
				return nil, "", mapError(ctx, err)
			}
			tupleValue, err := projectedTuple(value)
			return tupleValue, cursor, err
		}
		if exactObject && filter.Relation != "" && filter.User != "" {
			if condition == nil {
				result, err := datastore.client.ReadTupleExact(ctx, generated.ReadTupleExactParams{StoreId: store, ObjectType: objectType, ObjectId: objectID, Relation: filter.Relation, User: filter.User}, options)
				return finish(result.Value, "", err)
			}
			result, err := datastore.client.ReadTupleExactWithCondition(ctx, generated.ReadTupleExactWithConditionParams{StoreId: store, ObjectType: objectType, ObjectId: objectID, Relation: filter.Relation, User: filter.User, Condition: *condition}, options)
			return finish(result.Value, "", err)
		}
		if exactObject && filter.Relation != "" {
			if condition == nil {
				result, err := datastore.client.ReadTuplesByObjectRelation(ctx, generated.ReadTuplesByObjectRelationParams{StoreId: store, ObjectType: objectType, ObjectId: objectID, Relation: filter.Relation, After: after}, options)
				return finish(result.Value, result.NextCursor, err)
			}
			result, err := datastore.client.ReadTuplesByObjectRelationWithCondition(ctx, generated.ReadTuplesByObjectRelationWithConditionParams{StoreId: store, ObjectType: objectType, ObjectId: objectID, Relation: filter.Relation, Condition: *condition, After: after}, options)
			return finish(result.Value, result.NextCursor, err)
		}
		if exactObject && filter.User != "" {
			if condition == nil {
				result, err := datastore.client.ReadTuplesByObjectUser(ctx, generated.ReadTuplesByObjectUserParams{StoreId: store, ObjectType: objectType, ObjectId: objectID, User: filter.User, After: after}, options)
				return finish(result.Value, result.NextCursor, err)
			}
			result, err := datastore.client.ReadTuplesByObjectUserWithCondition(ctx, generated.ReadTuplesByObjectUserWithConditionParams{StoreId: store, ObjectType: objectType, ObjectId: objectID, User: filter.User, Condition: *condition, After: after}, options)
			return finish(result.Value, result.NextCursor, err)
		}
		if exactObject {
			if condition == nil {
				result, err := datastore.client.ReadTuplesByObject(ctx, generated.ReadTuplesByObjectParams{StoreId: store, ObjectType: objectType, ObjectId: objectID, After: after}, options)
				return finish(result.Value, result.NextCursor, err)
			}
			result, err := datastore.client.ReadTuplesByObjectWithCondition(ctx, generated.ReadTuplesByObjectWithConditionParams{StoreId: store, ObjectType: objectType, ObjectId: objectID, Condition: *condition, After: after}, options)
			return finish(result.Value, result.NextCursor, err)
		}
		if typeOnly && filter.Relation != "" && filter.User != "" {
			if condition == nil {
				result, err := datastore.client.ReadTuplesByTypeRelationUser(ctx, generated.ReadTuplesByTypeRelationUserParams{StoreId: store, ObjectType: objectType, Relation: filter.Relation, User: filter.User, After: after}, options)
				return finish(result.Value, result.NextCursor, err)
			}
			result, err := datastore.client.ReadTuplesByTypeRelationUserWithCondition(ctx, generated.ReadTuplesByTypeRelationUserWithConditionParams{StoreId: store, ObjectType: objectType, Relation: filter.Relation, User: filter.User, Condition: *condition, After: after}, options)
			return finish(result.Value, result.NextCursor, err)
		}
		if typeOnly && filter.Relation != "" {
			if condition == nil {
				result, err := datastore.client.ReadTuplesByTypeRelation(ctx, generated.ReadTuplesByTypeRelationParams{StoreId: store, ObjectType: objectType, Relation: filter.Relation, After: after}, options)
				return finish(result.Value, result.NextCursor, err)
			}
			result, err := datastore.client.ReadTuplesByTypeRelationWithCondition(ctx, generated.ReadTuplesByTypeRelationWithConditionParams{StoreId: store, ObjectType: objectType, Relation: filter.Relation, Condition: *condition, After: after}, options)
			return finish(result.Value, result.NextCursor, err)
		}
		if typeOnly && filter.User != "" {
			if condition == nil {
				result, err := datastore.client.ReadTuplesByTypeUser(ctx, generated.ReadTuplesByTypeUserParams{StoreId: store, ObjectType: objectType, User: filter.User, After: after}, options)
				return finish(result.Value, result.NextCursor, err)
			}
			result, err := datastore.client.ReadTuplesByTypeUserWithCondition(ctx, generated.ReadTuplesByTypeUserWithConditionParams{StoreId: store, ObjectType: objectType, User: filter.User, Condition: *condition, After: after}, options)
			return finish(result.Value, result.NextCursor, err)
		}
		if typeOnly {
			if condition == nil {
				result, err := datastore.client.ReadTuplesByType(ctx, generated.ReadTuplesByTypeParams{StoreId: store, ObjectType: objectType, After: after}, options)
				return finish(result.Value, result.NextCursor, err)
			}
			result, err := datastore.client.ReadTuplesByTypeWithCondition(ctx, generated.ReadTuplesByTypeWithConditionParams{StoreId: store, ObjectType: objectType, Condition: *condition, After: after}, options)
			return finish(result.Value, result.NextCursor, err)
		}
		if filter.Relation != "" && filter.User != "" {
			if condition == nil {
				result, err := datastore.client.ReadTuplesByRelationUser(ctx, generated.ReadTuplesByRelationUserParams{StoreId: store, Relation: filter.Relation, User: filter.User, After: after}, options)
				return finish(result.Value, result.NextCursor, err)
			}
			result, err := datastore.client.ReadTuplesByRelationUserWithCondition(ctx, generated.ReadTuplesByRelationUserWithConditionParams{StoreId: store, Relation: filter.Relation, User: filter.User, Condition: *condition, After: after}, options)
			return finish(result.Value, result.NextCursor, err)
		}
		if filter.User != "" {
			if condition == nil {
				result, err := datastore.client.ReadTuplesByUser(ctx, generated.ReadTuplesByUserParams{StoreId: store, User: filter.User, After: after}, options)
				return finish(result.Value, result.NextCursor, err)
			}
			result, err := datastore.client.ReadTuplesByUserWithCondition(ctx, generated.ReadTuplesByUserWithConditionParams{StoreId: store, User: filter.User, Condition: *condition, After: after}, options)
			return finish(result.Value, result.NextCursor, err)
		}
		if filter.Relation != "" {
			return nil, "", errors.New("riffdb-openfga: relation-only tuple reads are invalid")
		}
		if condition == nil {
			result, err := datastore.client.ReadAllTuples(ctx, generated.ReadAllTuplesParams{StoreId: store, After: after}, options)
			return finish(result.Value, result.NextCursor, err)
		}
		result, err := datastore.client.ReadAllTuplesWithConditions(ctx, generated.ReadAllTuplesWithConditionsParams{StoreId: store, Condition: *condition, After: after}, options)
		return finish(result.Value, result.NextCursor, err)
	}, nil
}

func (datastore *Datastore) tupleFetchers(store string, filter storage.ReadFilter) ([]tupleFetch, error) {
	conditions := conditionValues(filter.Conditions)
	fetchers := make([]tupleFetch, 0, len(conditions))
	for _, condition := range conditions {
		fetcher, err := datastore.tupleFetcher(store, filter, condition)
		if err != nil {
			return nil, err
		}
		fetchers = append(fetchers, fetcher)
	}
	return fetchers, nil
}

func (datastore *Datastore) Read(_ context.Context, store string, filter storage.ReadFilter, _ storage.ReadOptions) (storage.TupleIterator, error) {
	fetchers, err := datastore.tupleFetchers(store, filter)
	if err != nil {
		return nil, err
	}
	return &lazyTupleIterator{fetchers: fetchers, ordered: len(fetchers) == 1}, nil
}

type pageCursor struct {
	Version int    `json:"v"`
	Index   int    `json:"i"`
	Cursor  string `json:"c"`
}

func encodePageCursor(index int, cursor string) (string, error) {
	encoded, err := json.Marshal(pageCursor{Version: 1, Index: index, Cursor: cursor})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func decodePageCursor(value string, count int) (pageCursor, error) {
	if value == "" {
		return pageCursor{Version: 1}, nil
	}
	if len(value) > 22000 {
		return pageCursor{}, storage.ErrInvalidContinuationToken
	}
	encoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return pageCursor{}, storage.ErrInvalidContinuationToken
	}
	var cursor pageCursor
	if json.Unmarshal(encoded, &cursor) != nil || cursor.Version != 1 || cursor.Index < 0 || cursor.Index >= count || len(cursor.Cursor) > 16384 {
		return pageCursor{}, storage.ErrInvalidContinuationToken
	}
	return cursor, nil
}

func (datastore *Datastore) ReadPage(ctx context.Context, store string, filter storage.ReadFilter, options storage.ReadPageOptions) ([]*openfgav1.Tuple, string, error) {
	fetchers, err := datastore.tupleFetchers(store, filter)
	if err != nil {
		return nil, "", err
	}
	cursor, err := decodePageCursor(options.Pagination.From, len(fetchers))
	if err != nil {
		return nil, "", err
	}
	pageSize := options.Pagination.PageSize
	if pageSize <= 0 {
		pageSize = storage.DefaultPageSize
	}
	values := make([]*openfgav1.Tuple, 0, pageSize)
	index, after := cursor.Index, cursor.Cursor
	for len(values) < pageSize && index < len(fetchers) {
		var afterPtr *string
		if after != "" {
			afterPtr = &after
		}
		value, next, err := fetchers[index](ctx, afterPtr)
		if err != nil {
			return nil, "", err
		}
		if value != nil {
			values = append(values, value)
		}
		after = next
		if value == nil || next == "" {
			index++
			after = ""
		}
	}
	if index >= len(fetchers) {
		return values, "", nil
	}
	next, err := encodePageCursor(index, after)
	if err != nil {
		return nil, "", err
	}
	return values, next, nil
}

func (datastore *Datastore) ReadUserTuple(ctx context.Context, store string, filter storage.ReadUserTupleFilter, _ storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
	fetchers, err := datastore.tupleFetchers(store, filter)
	if err != nil {
		return nil, err
	}
	iterator := &lazyTupleIterator{fetchers: fetchers}
	value, err := iterator.Next(ctx)
	if errors.Is(err, storage.ErrIteratorDone) {
		return nil, storage.ErrNotFound
	}
	return value, err
}
