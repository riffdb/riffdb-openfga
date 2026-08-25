package riffdbopenfga

import (
	"context"
	"os"
	"strconv"
	"testing"

	storagetest "github.com/openfga/openfga/pkg/storage/test"
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
	storagetest.RunAllTests(t, datastore)
}
