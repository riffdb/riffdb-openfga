package riffdbopenfga

import (
	"context"
	"errors"
	"sort"
	"sync"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

type filteringTupleIterator struct {
	inner   storage.TupleIterator
	keep    func(*openfgav1.Tuple) bool
	mu      sync.Mutex
	head    *openfgav1.Tuple
	stopped bool
}

func (iterator *filteringTupleIterator) fetch(ctx context.Context) (*openfgav1.Tuple, error) {
	for {
		value, err := iterator.inner.Next(ctx)
		if err != nil {
			return nil, err
		}
		if iterator.keep(value) {
			return value, nil
		}
	}
}
func (iterator *filteringTupleIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	iterator.mu.Lock()
	defer iterator.mu.Unlock()
	if iterator.stopped {
		return nil, storage.ErrIteratorDone
	}
	if iterator.head != nil {
		value := iterator.head
		iterator.head = nil
		return value, nil
	}
	return iterator.fetch(ctx)
}
func (iterator *filteringTupleIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	iterator.mu.Lock()
	defer iterator.mu.Unlock()
	if iterator.stopped {
		return nil, storage.ErrIteratorDone
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
func (iterator *filteringTupleIterator) Stop() {
	iterator.mu.Lock()
	iterator.stopped = true
	iterator.head = nil
	iterator.inner.Stop()
	iterator.mu.Unlock()
}
func (iterator *filteringTupleIterator) IsOrdered() bool { return iterator.inner.IsOrdered() }

func allowedUserset(value *openfgav1.Tuple, restrictions []*openfgav1.RelationReference) bool {
	user := value.GetKey().GetUser()
	if tuple.GetUserTypeFromUser(user) != tuple.UserSet {
		return false
	}
	if len(restrictions) == 0 {
		return true
	}
	userObject, relation := tuple.SplitObjectRelation(user)
	userType, userID := tuple.SplitObject(userObject)
	for _, restriction := range restrictions {
		if restriction.GetType() != userType {
			continue
		}
		switch restriction.GetRelationOrWildcard().(type) {
		case *openfgav1.RelationReference_Relation:
			if restriction.GetRelation() == relation {
				return true
			}
		case *openfgav1.RelationReference_Wildcard:
			if userID == "*" && relation == "" {
				return true
			}
		}
	}
	return false
}

func (datastore *Datastore) ReadUsersetTuples(_ context.Context, store string, filter storage.ReadUsersetTuplesFilter, _ storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
	inner, err := datastore.Read(context.Background(), store, storage.ReadFilter{Object: filter.Object, Relation: filter.Relation, Conditions: filter.Conditions}, storage.ReadOptions{})
	if err != nil {
		return nil, err
	}
	return &filteringTupleIterator{inner: inner, keep: func(value *openfgav1.Tuple) bool { return allowedUserset(value, filter.AllowedUserTypeRestrictions) }}, nil
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
	iterators := make([]storage.TupleIterator, 0, len(users))
	for _, user := range users {
		inner, err := datastore.Read(context.Background(), store, storage.ReadFilter{Object: filter.ObjectType + ":", Relation: filter.Relation, User: user, Conditions: filter.Conditions}, storage.ReadOptions{})
		if err != nil {
			for _, existing := range iterators {
				existing.Stop()
			}
			return nil, err
		}
		if filter.ObjectIDs != nil {
			inner = &filteringTupleIterator{inner: inner, keep: func(value *openfgav1.Tuple) bool {
				_, id := tuple.SplitObject(value.GetKey().GetObject())
				return filter.ObjectIDs.Exists(id)
			}}
		}
		iterators = append(iterators, inner)
	}
	if options.WithResultsSortedAscending {
		return &mergingTupleIterator{iterators: iterators, heads: make([]*openfgav1.Tuple, len(iterators)), loaded: make([]bool, len(iterators))}, nil
	}
	return storage.NewCombinedIterator(iterators...), nil
}
