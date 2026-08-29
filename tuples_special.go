package riffdbopenfga

import (
	"context"
	"errors"
	"sort"
	"sync"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	generated "github.com/riffdb/riffdb-openfga/generated/go"
)

// usersetFetcher builds one bounded page source for userset tuples under an
// exact object and relation.
//
// `user_kind == "userset"` is applied by the query rather than by the caller,
// so the scan is bounded by the userset index instead of by an object/relation
// read the adapter then filters.
func (datastore *Datastore) usersetFetcher(
	store, objectType, objectID, relation string,
	userObjectType, userRelation *string,
	condition *string,
) tupleFetch {
	return func(ctx context.Context, limit uint32, after *string) ([]*openfgav1.Tuple, string, error) {
		options := generated.QueryOptions{}
		finish := func(value any, cursor string, err error) ([]*openfgav1.Tuple, string, error) {
			if err != nil {
				return nil, "", mapError(ctx, err)
			}
			page, err := projectedTuples(value)
			return page, cursor, err
		}
		if userObjectType != nil && userRelation != nil {
			if condition == nil {
				result, err := datastore.client.ReadUsersetByKindTypeRelation(ctx, generated.ReadUsersetByKindTypeRelationParams{StoreId: store, ObjectType: objectType, ObjectId: objectID, Relation: relation, UserKind: usersetUserKind, UserObjectType: *userObjectType, UserRelation: *userRelation, Limit: &limit, After: after}, options)
				return finish(result.Value, result.NextCursor, err)
			}
			result, err := datastore.client.ReadUsersetByKindTypeRelationWithCondition(ctx, generated.ReadUsersetByKindTypeRelationWithConditionParams{StoreId: store, ObjectType: objectType, ObjectId: objectID, Relation: relation, UserKind: usersetUserKind, UserObjectType: *userObjectType, UserRelation: *userRelation, Condition: *condition, Limit: &limit, After: after}, options)
			return finish(result.Value, result.NextCursor, err)
		}
		if condition == nil {
			result, err := datastore.client.ReadUsersetByKind(ctx, generated.ReadUsersetByKindParams{StoreId: store, ObjectType: objectType, ObjectId: objectID, Relation: relation, UserKind: usersetUserKind, Limit: &limit, After: after}, options)
			return finish(result.Value, result.NextCursor, err)
		}
		result, err := datastore.client.ReadUsersetByKindWithCondition(ctx, generated.ReadUsersetByKindWithConditionParams{StoreId: store, ObjectType: objectType, ObjectId: objectID, Relation: relation, UserKind: usersetUserKind, Condition: *condition, Limit: &limit, After: after}, options)
		return finish(result.Value, result.NextCursor, err)
	}
}

const usersetUserKind = "userset"

func (datastore *Datastore) ReadUsersetTuples(_ context.Context, store string, filter storage.ReadUsersetTuplesFilter, _ storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
	objectType, objectID := tuple.SplitObject(filter.Object)
	if objectType == "" || objectID == "" {
		return nil, errors.New("riffdb-openfga: userset reads require an exact object")
	}
	conditions := conditionValues(filter.Conditions)

	// `GetUserTypeFromUser` classifies both `type:id#relation` and `type:*` as
	// a userset, so both are stored with `user_kind = "userset"`. They differ in
	// `user_relation`: a userset carries its relation and a wildcard carries the
	// empty string. Both restriction kinds are therefore the same query with a
	// different relation, and neither needs a client-side filter.
	var allowed [][2]string
	for _, restriction := range filter.AllowedUserTypeRestrictions {
		switch restriction.GetRelationOrWildcard().(type) {
		case *openfgav1.RelationReference_Relation:
			allowed = append(allowed, [2]string{restriction.GetType(), restriction.GetRelation()})
		case *openfgav1.RelationReference_Wildcard:
			allowed = append(allowed, [2]string{restriction.GetType(), ""})
		}
	}

	var fetchers []tupleFetch
	if len(filter.AllowedUserTypeRestrictions) == 0 {
		for _, condition := range conditions {
			fetchers = append(fetchers, datastore.usersetFetcher(store, objectType, objectID, filter.Relation, nil, nil, condition))
		}
	} else {
		for _, pair := range allowed {
			userObjectType, userRelation := pair[0], pair[1]
			for _, condition := range conditions {
				fetchers = append(fetchers, datastore.usersetFetcher(store, objectType, objectID, filter.Relation, &userObjectType, &userRelation, condition))
			}
		}
	}
	return &lazyTupleIterator{fetchers: fetchers, ordered: len(fetchers) == 1}, nil
}

type mergingTupleIterator struct {
	mu        sync.Mutex
	iterators []storage.TupleIterator
	heads     []*openfgav1.Tuple
	loaded    []bool
	stopped   bool
}

func tupleOrder(value *openfgav1.Tuple) string {
	return value.GetKey().GetObject() + "\x00" + value.GetKey().GetRelation() + "\x00" + value.GetKey().GetUser()
}
func (iterator *mergingTupleIterator) load(ctx context.Context) error {
	for index, inner := range iterator.iterators {
		if iterator.loaded[index] {
			continue
		}
		value, err := inner.Head(ctx)
		if errors.Is(err, storage.ErrIteratorDone) {
			iterator.loaded[index] = true
			continue
		}
		if err != nil {
			return err
		}
		iterator.heads[index] = value
		iterator.loaded[index] = true
	}
	return nil
}
func (iterator *mergingTupleIterator) selected(ctx context.Context) (int, *openfgav1.Tuple, error) {
	if err := iterator.load(ctx); err != nil {
		return -1, nil, err
	}
	selected := -1
	for index, value := range iterator.heads {
		if value != nil && (selected < 0 || tupleOrder(value) < tupleOrder(iterator.heads[selected])) {
			selected = index
		}
	}
	if selected < 0 {
		return -1, nil, storage.ErrIteratorDone
	}
	return selected, iterator.heads[selected], nil
}
func (iterator *mergingTupleIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	iterator.mu.Lock()
	defer iterator.mu.Unlock()
	if iterator.stopped {
		return nil, storage.ErrIteratorDone
	}
	index, value, err := iterator.selected(ctx)
	if err != nil {
		return nil, err
	}
	_, err = iterator.iterators[index].Next(ctx)
	if err != nil {
		return nil, err
	}
	iterator.heads[index] = nil
	iterator.loaded[index] = false
	return value, nil
}
func (iterator *mergingTupleIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	iterator.mu.Lock()
	defer iterator.mu.Unlock()
	if iterator.stopped {
		return nil, storage.ErrIteratorDone
	}
	_, value, err := iterator.selected(ctx)
	return value, err
}
func (iterator *mergingTupleIterator) Stop() {
	iterator.mu.Lock()
	iterator.stopped = true
	for _, inner := range iterator.iterators {
		inner.Stop()
	}
	iterator.mu.Unlock()
}
func (*mergingTupleIterator) IsOrdered() bool { return true }

// startingWithUserFetcher builds one bounded page source for reverse reads.
//
// When the caller supplies object IDs the query applies `object_id in
// $object_ids`, so the set bounds the scan instead of filtering its output.
// Every variant orders by `object_id asc`, which is the order
// `WithResultsSortedAscending` needs.
func (datastore *Datastore) startingWithUserFetcher(
	store, objectType, relation, user string,
	objectIDs []string,
	condition *string,
) tupleFetch {
	return func(ctx context.Context, limit uint32, after *string) ([]*openfgav1.Tuple, string, error) {
		options := generated.QueryOptions{}
		finish := func(value any, cursor string, err error) ([]*openfgav1.Tuple, string, error) {
			if err != nil {
				return nil, "", mapError(ctx, err)
			}
			page, err := projectedTuples(value)
			return page, cursor, err
		}
		if objectIDs != nil {
			if condition == nil {
				result, err := datastore.client.ReadStartingWithUserAndObjects(ctx, generated.ReadStartingWithUserAndObjectsParams{StoreId: store, ObjectType: objectType, Relation: relation, User: user, ObjectIds: objectIDs, Limit: &limit, After: after}, options)
				return finish(result.Value, result.NextCursor, err)
			}
			result, err := datastore.client.ReadStartingWithUserObjectsCondition(ctx, generated.ReadStartingWithUserObjectsConditionParams{StoreId: store, ObjectType: objectType, Relation: relation, User: user, ObjectIds: objectIDs, Condition: *condition, Limit: &limit, After: after}, options)
			return finish(result.Value, result.NextCursor, err)
		}
		if condition == nil {
			result, err := datastore.client.ReadStartingWithUser(ctx, generated.ReadStartingWithUserParams{StoreId: store, ObjectType: objectType, Relation: relation, User: user, Limit: &limit, After: after}, options)
			return finish(result.Value, result.NextCursor, err)
		}
		result, err := datastore.client.ReadStartingWithUserWithCondition(ctx, generated.ReadStartingWithUserWithConditionParams{StoreId: store, ObjectType: objectType, Relation: relation, User: user, Condition: *condition, Limit: &limit, After: after}, options)
		return finish(result.Value, result.NextCursor, err)
	}
}

func (datastore *Datastore) ReadStartingWithUser(_ context.Context, store string, filter storage.ReadStartingWithUserFilter, options storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
	users := make([]string, 0, len(filter.UserFilter))
	seen := make(map[string]struct{}, len(filter.UserFilter))
	for _, value := range filter.UserFilter {
		user := tuple.GetObjectRelationAsString(value)
		if _, ok := seen[user]; !ok {
			users = append(users, user)
			seen[user] = struct{}{}
		}
	}
	sort.Strings(users)

	var objectIDs []string
	if filter.ObjectIDs != nil {
		objectIDs = filter.ObjectIDs.Values()
		if len(objectIDs) == 0 {
			// An empty set selects nothing, and the compiled query requires a
			// non-empty set, so answer without asking.
			return storage.NewCombinedIterator[*openfgav1.Tuple](), nil
		}
	}

	conditions := conditionValues(filter.Conditions)
	iterators := make([]storage.TupleIterator, 0, len(users))
	for _, user := range users {
		fetchers := make([]tupleFetch, 0, len(conditions))
		for _, condition := range conditions {
			fetchers = append(fetchers, datastore.startingWithUserFetcher(store, filter.ObjectType, filter.Relation, user, objectIDs, condition))
		}
		iterators = append(iterators, &lazyTupleIterator{fetchers: fetchers, ordered: len(fetchers) == 1})
	}
	if options.WithResultsSortedAscending {
		return &mergingTupleIterator{iterators: iterators, heads: make([]*openfgav1.Tuple, len(iterators)), loaded: make([]bool, len(iterators))}, nil
	}
	return storage.NewCombinedIterator(iterators...), nil
}
