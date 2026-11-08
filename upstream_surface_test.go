package riffdbopenfga

import (
	"encoding/json"
	"os"
	"reflect"
	"slices"
	"testing"

	"github.com/openfga/openfga/pkg/storage"
)

func TestLockedIdentityConstantsMatchLockFile(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile("riffdb.application.lock.json")
	if err != nil {
		t.Fatalf("read lock file: %v", err)
	}

	var lock struct {
		Artifacts []struct {
			Kind        string `json:"kind"`
			ContentHash string `json:"content_hash"`
		} `json:"artifacts"`
		ExactManifestHash string `json:"exact_manifest_hash"`
		Contract          struct {
			BundleHash string `json:"bundle_hash"`
			Lineage    string `json:"lineage"`
			Version    uint64 `json:"version"`
		} `json:"contract"`
		Modules []struct {
			Name       string `json:"name"`
			ModuleHash string `json:"module_hash"`
		} `json:"modules"`
		Roles []struct {
			Definition struct {
				Name string `json:"name"`
			} `json:"definition"`
			DefinitionHash string `json:"definition_hash"`
		} `json:"roles"`
	}
	if err := json.Unmarshal(raw, &lock); err != nil {
		t.Fatalf("unmarshal lock file: %v", err)
	}

	var mcpCatalogHash string
	for _, artifact := range lock.Artifacts {
		if artifact.Kind == "mcp" {
			mcpCatalogHash = artifact.ContentHash
			break
		}
	}
	if mcpCatalogHash != OperationCatalogHash {
		t.Fatalf("OperationCatalogHash mismatch: got %q, lock mcp artifact has %q", OperationCatalogHash, mcpCatalogHash)
	}

	if lock.ExactManifestHash != ApplicationManifestHash {
		t.Fatalf("ApplicationManifestHash mismatch: got %q, lock has %q", ApplicationManifestHash, lock.ExactManifestHash)
	}
	if lock.Contract.BundleHash != ContractBundleHash {
		t.Fatalf("ContractBundleHash mismatch: got %q, lock has %q", ContractBundleHash, lock.Contract.BundleHash)
	}
	if lock.Contract.Lineage != ContractLineage {
		t.Fatalf("ContractLineage mismatch: got %q, lock has %q", ContractLineage, lock.Contract.Lineage)
	}
	if lock.Contract.Version != ContractVersion {
		t.Fatalf("ContractVersion mismatch: got %d, lock has %d", ContractVersion, lock.Contract.Version)
	}
	if len(lock.Modules) != 1 || lock.Modules[0].ModuleHash != QueryModuleHash {
		t.Fatalf("QueryModuleHash mismatch: got %q, lock modules %+v", QueryModuleHash, lock.Modules)
	}
	if len(lock.Roles) != 1 || lock.Roles[0].Definition.Name != DefaultRole || lock.Roles[0].DefinitionHash != DefaultRoleDefinitionHash {
		t.Fatalf("DefaultRole/DefaultRoleDefinitionHash mismatch: got %q/%q, lock roles %+v", DefaultRole, DefaultRoleDefinitionHash, lock.Roles)
	}
}

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
