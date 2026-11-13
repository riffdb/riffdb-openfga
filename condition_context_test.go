package riffdbopenfga

import (
	"reflect"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage"
	generated "github.com/riffdb/riffdb-openfga/generated/go"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestShouldRetryPolicyWriteMatchesTupleWriteOptions(t *testing.T) {
	t.Parallel()

	writeUnexpected := generated.ApplyTupleMutationsTupleStateUnexpected{
		ObjectType: "document",
		ObjectId:   "roadmap",
		Relation:   "viewer",
		User:       "user:anne",
		Operation:  generated.TupleOperationWrite,
	}
	deleteUnexpected := generated.ApplyTupleMutationsTupleStateUnexpected{
		ObjectType: "document",
		ObjectId:   "roadmap",
		Relation:   "viewer",
		User:       "user:anne",
		Operation:  generated.TupleOperationDelete,
	}

	if shouldRetryPolicyWrite(writeUnexpected, storage.NewTupleWriteOptions()) {
		t.Fatal("default OnDuplicateInsertError must not retry unexpected write state")
	}
	if !shouldRetryPolicyWrite(writeUnexpected, storage.NewTupleWriteOptions(storage.WithOnDuplicateInsert(storage.OnDuplicateInsertIgnore))) {
		t.Fatal("OnDuplicateInsertIgnore must retry unexpected write state after concurrent insert")
	}
	if shouldRetryPolicyWrite(deleteUnexpected, storage.NewTupleWriteOptions()) {
		t.Fatal("default OnMissingDeleteError must not retry unexpected delete state")
	}
	if !shouldRetryPolicyWrite(deleteUnexpected, storage.NewTupleWriteOptions(storage.WithOnMissingDelete(storage.OnMissingDeleteIgnore))) {
		t.Fatal("OnMissingDeleteIgnore must retry unexpected delete state after concurrent delete")
	}
}

func TestConditionContextEncodingPreservesEmptyPresence(t *testing.T) {
	t.Parallel()

	empty := new(structpb.Struct)
	encoded, err := encodeConditionContext(empty)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) == 0 {
		t.Fatal("empty protobuf context must have a non-empty persisted encoding")
	}
	decoded, err := decodeConditionContext(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded == nil || len(decoded.GetFields()) != 0 {
		t.Fatalf("empty context did not round-trip: %#v", decoded)
	}

	nonEmpty, err := structpb.NewStruct(map[string]any{"region": "us-east"})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err = encodeConditionContext(nonEmpty)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err = decodeConditionContext(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded.AsMap(), nonEmpty.AsMap()) {
		t.Fatalf("non-empty context did not round-trip: got %v, want %v", decoded.AsMap(), nonEmpty.AsMap())
	}
}

func TestConditionWithoutContextNormalizesToPersistedEmptyContext(t *testing.T) {
	t.Parallel()

	name, encoded, err := encodeCondition(&openfgav1.RelationshipCondition{Name: "condition"})
	if err != nil {
		t.Fatal(err)
	}
	if name != "condition" || encoded == nil || !reflect.DeepEqual(*encoded, emptyConditionContextEncoding) {
		t.Fatalf("condition context was not normalized: name=%q encoded=%v", name, encoded)
	}
}
