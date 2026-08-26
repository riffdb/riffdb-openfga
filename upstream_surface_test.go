package riffdbopenfga

import (
	"reflect"
	"slices"
	"testing"

	"github.com/openfga/openfga/pkg/storage"
)

func TestPinnedOpenFGADatastoreSurface(t *testing.T) {
	t.Parallel()

	want := []string{
		"Close",
		"CreateStore",
		"DeleteStore",
		"FindLatestAuthorizationModel",
		"GetStore",
		"IsReady",
		"ListStores",
		"MaxTuplesPerWrite",
		"MaxTypesPerAuthorizationModel",
		"Read",
		"ReadAssertions",
		"ReadAuthorizationModel",
		"ReadAuthorizationModels",
		"ReadChanges",
		"ReadPage",
		"ReadStartingWithUser",
		"ReadUserTuple",
		"ReadUsersetTuples",
		"Write",
		"WriteAssertions",
		"WriteAuthorizationModel",
	}

	surface := reflect.TypeOf((*storage.OpenFGADatastore)(nil)).Elem()
	got := make([]string, 0, surface.NumMethod())
	for index := range surface.NumMethod() {
		got = append(got, surface.Method(index).Name)
	}

	if !slices.Equal(got, want) {
		t.Fatalf("OpenFGADatastore method surface changed\n got: %v\nwant: %v", got, want)
	}
}

func TestPinnedOpenFGADefaults(t *testing.T) {
	t.Parallel()

	if storage.DefaultMaxTuplesPerWrite != MaxTuplesPerWrite {
		t.Fatalf("tuple limit changed: got %d, want %d", storage.DefaultMaxTuplesPerWrite, MaxTuplesPerWrite)
	}
	if storage.DefaultMaxTypesPerAuthorizationModel != MaxTypesPerAuthorizationModel {
		t.Fatalf("model type limit changed: got %d, want %d", storage.DefaultMaxTypesPerAuthorizationModel, MaxTypesPerAuthorizationModel)
	}
	if storage.DefaultPageSize != DefaultPageSize {
		t.Fatalf("page default changed: got %d, want %d", storage.DefaultPageSize, DefaultPageSize)
	}
}
