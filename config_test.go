package riffdbopenfga

import (
	"testing"

	riffdb "riffdb.dev/application"
)

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{name: "valid", config: Config{Session: &riffdb.Session{}, CommandAttempts: 3}},
		{name: "missing session", config: Config{CommandAttempts: 3}, wantErr: true},
		{name: "zero attempts", config: Config{Session: &riffdb.Session{}}, wantErr: true},
		{name: "too many attempts", config: Config{Session: &riffdb.Session{}, CommandAttempts: 11}, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := test.config.Validate()
			if (err != nil) != test.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func TestDefaultIdentityAndConnectConfig(t *testing.T) {
	identity := DefaultIdentity("", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	if identity.Database != DefaultDatabase {
		t.Fatalf("Database = %q, want %q", identity.Database, DefaultDatabase)
	}
	if identity.ApplicationManifestHash != ApplicationManifestHash {
		t.Fatalf("ApplicationManifestHash = %q, want %q", identity.ApplicationManifestHash, ApplicationManifestHash)
	}
	if identity.Role != DefaultRole || identity.RoleDefinitionHash != DefaultRoleDefinitionHash {
		t.Fatalf("Role/Hash = %q/%q", identity.Role, identity.RoleDefinitionHash)
	}
	if got := NormalizeSocketPath("unix:///tmp/riffdb.sock"); got != "/tmp/riffdb.sock" {
		t.Fatalf("NormalizeSocketPath = %q, want /tmp/riffdb.sock", got)
	}

	t.Setenv("OPENFGA_RIFFDB_SOCKET", "unix:///tmp/env.sock")
	t.Setenv("OPENFGA_RIFFDB_DATABASE", "custom-db")
	t.Setenv("OPENFGA_RIFFDB_REMOTE_IDENTITY_HASH", "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210")

	cfg, err := ConnectConfigFromEnv()
	if err != nil {
		t.Fatalf("ConnectConfigFromEnv() error = %v", err)
	}
	if cfg.SocketPath != "/tmp/env.sock" || cfg.Database != "custom-db" {
		t.Fatalf("unexpected ConnectConfigFromEnv: %+v", cfg)
	}
}
