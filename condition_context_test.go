package riffdbopenfga

import (
	"reflect"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

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
