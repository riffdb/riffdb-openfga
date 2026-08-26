package riffdbopenfga

import (
	"bytes"
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	storagetest "github.com/openfga/openfga/pkg/storage/test"
	"github.com/openfga/openfga/pkg/tuple"
	generated "github.com/riffdb/riffdb-openfga/generated/go"
	riffdb "riffdb.dev/application"
)

func TestLoopbackConformance(t *testing.T) {
	socket := os.Getenv("RIFFDB_LOOPBACK_SOCKET")
	if socket == "" {
		t.Skip("run through riffdb dev --run with the loopback test shim")
	}
	version, err := strconv.ParseUint(os.Getenv("RIFFDB_LOOPBACK_VERSION"), 10, 64)
	if err != nil || version == 0 {
		t.Fatal("invalid loopback contract version")
	}
	session, err := riffdb.Connect(context.Background(), socket, riffdb.Identity{
		ApplicationManifestHash: os.Getenv("RIFFDB_LOOPBACK_MANIFEST_HASH"),
		OperationCatalogHash:    os.Getenv("RIFFDB_LOOPBACK_CATALOG_HASH"),
		Database:                os.Getenv("RIFFDB_LOOPBACK_DATABASE"),
		Role:                    os.Getenv("RIFFDB_LOOPBACK_ROLE"),
		RoleDefinitionHash:      os.Getenv("RIFFDB_LOOPBACK_ROLE_HASH"),
		RemoteIdentityHash:      os.Getenv("RIFFDB_LOOPBACK_REMOTE_HASH"),
		ContractLineage:         os.Getenv("RIFFDB_LOOPBACK_LINEAGE"),
		ContractVersion:         version,
		ContractBundleHash:      os.Getenv("RIFFDB_LOOPBACK_BUNDLE_HASH"),
	})
	if err != nil {
		t.Fatal(err)
	}
	datastore, err := NewDatastore(Config{Session: session, CommandAttempts: 3, CloseSession: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(datastore.Close)
	if generated.ContractLineage != os.Getenv("RIFFDB_LOOPBACK_LINEAGE") {
		t.Fatal("generated contract lineage mismatch")
	}
	t.Run("RiffDBEmptyConditionContextRoundTrip", func(t *testing.T) {
		key := &openfgav1.TupleKey{
			Object:   "document:empty-context",
			Relation: "viewer",
			User:     "user:anne",
			Condition: &openfgav1.RelationshipCondition{
				Name: "condition",
			},
		}
		if err := datastore.Write(context.Background(), "empty-context-store", nil, []*openfgav1.TupleKey{key}); err != nil {
			t.Fatal(err)
		}
		state, found, err := datastore.getTupleState(context.Background(), tupleParts("empty-context-store", tuple.TupleKeyToTupleKeyWithoutCondition(key)))
		if err != nil {
			t.Fatal(err)
		}
		if !found || state.conditionContext == nil || !bytes.Equal(*state.conditionContext, emptyConditionContextEncoding) {
			t.Fatalf("empty condition context encoding was not preserved: found=%v context=%v", found, state.conditionContext)
		}
		result, err := datastore.client.ReadChangesAsc(context.Background(), generated.ReadChangesAscParams{
			StoreId: "empty-context-store",
			Horizon: instant(time.Now().UTC().Add(time.Minute)),
		}, generated.QueryOptions{})
		if err != nil {
			t.Fatal(err)
		}
		changes, ok := result.Value.(generated.ReadChangesAscFound)
		if !ok || len(changes.Changes) != 1 {
			t.Fatalf("unexpected changelog projection: %#v", result.Value)
		}
		if changes.Changes[0].ConditionContext == nil || !bytes.Equal(*changes.Changes[0].ConditionContext, emptyConditionContextEncoding) {
			t.Fatalf("empty condition context was not preserved in changelog: %v", changes.Changes[0].ConditionContext)
		}
		projected, _, err := projectedChange(result.Value)
		if err != nil {
			t.Fatal(err)
		}
		if projected.GetTupleKey().GetCondition().GetContext() == nil {
			t.Fatal("empty condition context presence was lost during projection")
		}
	})
	storagetest.RunAllTests(t, datastore)
}
