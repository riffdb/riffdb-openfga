package riffdbopenfga

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"

	generated "github.com/riffdb/riffdb-openfga/generated/go"
	riffdb "riffdb.dev/application"
)

const (
	// ApplicationManifestHash is the exact manifest hash recorded in
	// riffdb.application.lock.json for this compiled application.
	ApplicationManifestHash = "3b79649361a51a04ff3d148ae949c6de39dd2a9ba2153757f4a69192939dc11c"

	// OperationCatalogHash is the exact operation catalog (MCP tools.json)
	// content hash recorded in riffdb.application.lock.json for this
	// compiled application.
	OperationCatalogHash = "950b0670c6d7080352fbb7766b79184f352c96625401fdcdce6f40290fd03b30"

	// QueryModuleHash is the compiled RiffQL query module hash for this
	// application.
	QueryModuleHash = generated.QueryModuleHash

	// ContractLineage is the RiffDB contract lineage name for this adapter.
	ContractLineage = generated.ContractLineage

	// ContractVersion is the compiled RiffDB contract version for this adapter.
	ContractVersion = generated.ContractVersion

	// ContractBundleHash is the compiled RiffDB contract bundle hash.
	ContractBundleHash = generated.ContractBundleHash

	// DefaultRole is the application role granted OpenFGA query and command
	// authority in riffdb.application.json.
	DefaultRole = "OpenFGAServer"

	// DefaultRoleDefinitionHash is the exact role definition hash recorded in
	// riffdb.application.lock.json for DefaultRole.
	DefaultRoleDefinitionHash = "124f1a8b580fb7c08c44cbbba8e29dacac34355a35a8eb21dddb503acc6c232e"

	// DefaultDatabase is the default database name configured in riffdb.toml.
	DefaultDatabase = "openfga-development"
)

// LoopbackEnvironmentVariables lists the 10 positional identity values passed
// by `riffdb dev --run` to a Go runner package.
var LoopbackEnvironmentVariables = [...]string{
	"RIFFDB_LOOPBACK_SOCKET",
	"RIFFDB_LOOPBACK_MANIFEST_HASH",
	"RIFFDB_LOOPBACK_CATALOG_HASH",
	"RIFFDB_LOOPBACK_DATABASE",
	"RIFFDB_LOOPBACK_ROLE",
	"RIFFDB_LOOPBACK_ROLE_HASH",
	"RIFFDB_LOOPBACK_REMOTE_HASH",
	"RIFFDB_LOOPBACK_LINEAGE",
	"RIFFDB_LOOPBACK_VERSION",
	"RIFFDB_LOOPBACK_BUNDLE_HASH",
}

// ConnectConfig configures a RiffDB driver session for the compiled OpenFGA
// application. Zero-valued compile-time identity fields default to the
// constants locked in this package.
type ConnectConfig struct {
	SocketPath              string
	Database                string
	Role                    string
	RoleDefinitionHash      string
	OperationCatalogHash    string
	RemoteIdentityHash      string
	ApplicationManifestHash string
	ContractLineage         string
	ContractVersion         uint64
	ContractBundleHash      string
	Tenant                  string
}

// DefaultIdentity returns a riffdb.Identity pre-populated with this compiled
// application's locked manifest, contract, and role hashes.
func DefaultIdentity(database, remoteIdentityHash string) riffdb.Identity {
	if database == "" {
		database = DefaultDatabase
	}
	return riffdb.Identity{
		ApplicationManifestHash: ApplicationManifestHash,
		OperationCatalogHash:    OperationCatalogHash,
		ContractLineage:         ContractLineage,
		ContractVersion:         ContractVersion,
		ContractBundleHash:      ContractBundleHash,
		Database:                database,
		Role:                    DefaultRole,
		RoleDefinitionHash:      DefaultRoleDefinitionHash,
		RemoteIdentityHash:      remoteIdentityHash,
	}
}

// ConnectConfigFromEnv reads RiffDB connection and identity settings from
// OPENFGA_RIFFDB_*, RIFFDB_*, and RIFFDB_LOOPBACK_* environment variables,
// falling back to the compiled application's locked constants.
func ConnectConfigFromEnv() (ConnectConfig, error) {
	version := uint64(ContractVersion)
	if rawVersion := firstNonEmpty(
		os.Getenv("OPENFGA_RIFFDB_CONTRACT_VERSION"),
		os.Getenv("RIFFDB_CONTRACT_VERSION"),
		os.Getenv("RIFFDB_LOOPBACK_VERSION"),
	); rawVersion != "" {
		parsed, err := strconv.ParseUint(rawVersion, 10, 64)
		if err != nil || parsed == 0 {
			return ConnectConfig{}, errors.New("riffdb-openfga: invalid contract version")
		}
		version = parsed
	}

	return ConnectConfig{
		SocketPath: NormalizeSocketPath(firstNonEmpty(
			os.Getenv("OPENFGA_RIFFDB_SOCKET"),
			os.Getenv("RIFFDB_DRIVER_SOCKET"),
			os.Getenv("RIFFDB_SOCKET"),
			os.Getenv("RIFFDB_LOOPBACK_SOCKET"),
		)),
		Database: firstNonEmpty(
			os.Getenv("OPENFGA_RIFFDB_DATABASE"),
			os.Getenv("RIFFDB_DATABASE"),
			os.Getenv("RIFFDB_LOOPBACK_DATABASE"),
			DefaultDatabase,
		),
		Role: firstNonEmpty(
			os.Getenv("OPENFGA_RIFFDB_ROLE"),
			os.Getenv("RIFFDB_ROLE"),
			os.Getenv("RIFFDB_LOOPBACK_ROLE"),
			DefaultRole,
		),
		RoleDefinitionHash: firstNonEmpty(
			os.Getenv("OPENFGA_RIFFDB_ROLE_HASH"),
			os.Getenv("RIFFDB_ROLE_HASH"),
			os.Getenv("RIFFDB_LOOPBACK_ROLE_HASH"),
			DefaultRoleDefinitionHash,
		),
		OperationCatalogHash: firstNonEmpty(
			os.Getenv("OPENFGA_RIFFDB_CATALOG_HASH"),
			os.Getenv("RIFFDB_CATALOG_HASH"),
			os.Getenv("RIFFDB_LOOPBACK_CATALOG_HASH"),
			OperationCatalogHash,
		),
		RemoteIdentityHash: firstNonEmpty(
			os.Getenv("OPENFGA_RIFFDB_REMOTE_IDENTITY_HASH"),
			os.Getenv("RIFFDB_REMOTE_IDENTITY_HASH"),
			os.Getenv("RIFFDB_REMOTE_HASH"),
			os.Getenv("RIFFDB_LOOPBACK_REMOTE_HASH"),
		),
		ApplicationManifestHash: firstNonEmpty(
			os.Getenv("OPENFGA_RIFFDB_MANIFEST_HASH"),
			os.Getenv("RIFFDB_MANIFEST_HASH"),
			os.Getenv("RIFFDB_LOOPBACK_MANIFEST_HASH"),
			ApplicationManifestHash,
		),
		ContractLineage: firstNonEmpty(
			os.Getenv("OPENFGA_RIFFDB_LINEAGE"),
			os.Getenv("RIFFDB_LINEAGE"),
			os.Getenv("RIFFDB_LOOPBACK_LINEAGE"),
			ContractLineage,
		),
		ContractVersion: version,
		ContractBundleHash: firstNonEmpty(
			os.Getenv("OPENFGA_RIFFDB_BUNDLE_HASH"),
			os.Getenv("RIFFDB_BUNDLE_HASH"),
			os.Getenv("RIFFDB_LOOPBACK_BUNDLE_HASH"),
			ContractBundleHash,
		),
		Tenant: firstNonEmpty(
			os.Getenv("OPENFGA_RIFFDB_TENANT"),
			os.Getenv("RIFFDB_TENANT"),
		),
	}, nil
}

// NormalizeSocketPath strips an optional unix:// scheme prefix from a socket
// URI or path.
func NormalizeSocketPath(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if strings.HasPrefix(trimmed, "unix://") {
		return strings.TrimPrefix(trimmed, "unix://")
	}
	return trimmed
}

// Identity resolves the ConnectConfig into a riffdb.Identity, filling any
// omitted compile-time fields with the locked package defaults.
func (cfg ConnectConfig) Identity() riffdb.Identity {
	identity := DefaultIdentity(cfg.Database, cfg.RemoteIdentityHash)
	if cfg.ApplicationManifestHash != "" {
		identity.ApplicationManifestHash = cfg.ApplicationManifestHash
	}
	if cfg.OperationCatalogHash != "" {
		identity.OperationCatalogHash = cfg.OperationCatalogHash
	}
	if cfg.ContractLineage != "" {
		identity.ContractLineage = cfg.ContractLineage
	}
	if cfg.ContractVersion != 0 {
		identity.ContractVersion = cfg.ContractVersion
	}
	if cfg.ContractBundleHash != "" {
		identity.ContractBundleHash = cfg.ContractBundleHash
	}
	if cfg.Database != "" {
		identity.Database = cfg.Database
	}
	if cfg.Role != "" {
		identity.Role = cfg.Role
	}
	if cfg.RoleDefinitionHash != "" {
		identity.RoleDefinitionHash = cfg.RoleDefinitionHash
	}
	if cfg.Tenant != "" {
		identity.Tenant = cfg.Tenant
	}
	return identity
}

// Connect establishes an authenticated RiffDB driver session using cfg.
func Connect(ctx context.Context, cfg ConnectConfig) (*riffdb.Session, error) {
	socketPath := NormalizeSocketPath(cfg.SocketPath)
	if socketPath == "" {
		return nil, errors.New("riffdb-openfga: driver socket path is required")
	}
	identity := cfg.Identity()
	if identity.ContractLineage != ContractLineage {
		return nil, errors.New("riffdb-openfga: contract lineage mismatch")
	}
	return riffdb.Connect(ctx, socketPath, identity)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
