package openfgarun

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/logger"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	riffdbopenfga "github.com/riffdb/riffdb-openfga"
)

func freeTCPAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())
	return addr
}

func TestReadConfigAndRiffDBConfigFromFlagsEnvAndYAML(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)
	cfgDir := filepath.Join(tempHome, ".openfga")
	require.NoError(t, os.MkdirAll(cfgDir, 0o750))

	yamlContent := `
grpc:
  addr: 127.0.0.1:18081
http:
  enabled: true
  addr: 127.0.0.1:18080
authn:
  method: preshared
  preshared:
    keys:
      - yaml-key-1
datastore:
  engine: riffdb
  uri: unix:///tmp/yaml-riffdb.sock
metrics:
  enabled: true
  enableRPCHistograms: true
`
	require.NoError(t, os.WriteFile(filepath.Join(cfgDir, "config.yaml"), []byte(yamlContent), 0o600))

	t.Setenv("OPENFGA_MAX_TUPLES_PER_WRITE", "250")
	t.Setenv("OPENFGA_RIFFDB_DATABASE", "openfga-prod")

	cmd := NewRunCommand()
	require.NoError(t, cmd.ParseFlags([]string{
		"--riffdb-command-attempts=5",
		"--playground-enabled=false",
	}))
	cmd.PreRun(cmd, nil)

	cfg, err := ReadConfig()
	require.NoError(t, err)
	require.Equal(t, "riffdb", cfg.Datastore.Engine)
	require.Equal(t, "127.0.0.1:18081", cfg.GRPC.Addr)
	require.Equal(t, "127.0.0.1:18080", cfg.HTTP.Addr)
	require.Equal(t, "preshared", cfg.Authn.Method)
	require.Equal(t, []string{"yaml-key-1"}, cfg.Authn.Keys)
	require.Equal(t, 250, cfg.MaxTuplesPerWrite)
	require.True(t, cfg.Metrics.Enabled)
	require.True(t, cfg.Metrics.EnableRPCHistograms)

	riffCfg, err := ReadRiffDBConfig(cfg)
	require.NoError(t, err)
	require.Equal(t, "/tmp/yaml-riffdb.sock", riffCfg.SocketPath)
	require.Equal(t, "openfga-prod", riffCfg.Database)
	require.Equal(t, uint32(5), riffCfg.CommandAttempts)
	require.Equal(t, riffdbopenfga.ApplicationManifestHash, riffCfg.ApplicationManifestHash)
	require.Equal(t, riffdbopenfga.OperationCatalogHash, riffCfg.OperationCatalogHash)
	require.Equal(t, riffdbopenfga.ContractBundleHash, riffCfg.ContractBundleHash)
	require.Equal(t, riffdbopenfga.DefaultRole, riffCfg.Role)
	require.Equal(t, riffdbopenfga.DefaultRoleDefinitionHash, riffCfg.RoleDefinitionHash)
}

func TestApplyLoopbackArgs(t *testing.T) {
	loopbackArgs := []string{
		"/tmp/riffdb.sock",
		riffdbopenfga.ApplicationManifestHash,
		riffdbopenfga.OperationCatalogHash,
		"openfga-dev",
		riffdbopenfga.DefaultRole,
		riffdbopenfga.DefaultRoleDefinitionHash,
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		riffdbopenfga.ContractLineage,
		"1",
		riffdbopenfga.ContractBundleHash,
	}

	for _, key := range riffdbopenfga.LoopbackEnvironmentVariables {
		t.Setenv(key, "")
	}

	rewritten, ok := ApplyLoopbackArgs(loopbackArgs)
	require.True(t, ok)
	require.Equal(t, []string{"run"}, rewritten)
	require.Equal(t, "/tmp/riffdb.sock", os.Getenv("RIFFDB_LOOPBACK_SOCKET"))
	require.Equal(t, "openfga-dev", os.Getenv("RIFFDB_LOOPBACK_DATABASE"))

	_, ok = ApplyLoopbackArgs([]string{"run", "--http-addr=127.0.0.1:8080"})
	require.False(t, ok)
}

func TestRemoteOidcAuthenticator(t *testing.T) {
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	kid := "test-key-1"

	var serverURL string
	oidcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"issuer":   serverURL,
				"jwks_uri": serverURL + "/jwks.json",
			})
		case "/jwks.json":
			nBytes := privKey.PublicKey.N.Bytes()
			eBytes := big.NewInt(int64(privKey.PublicKey.E)).Bytes()
			_ = json.NewEncoder(w).Encode(map[string]any{
				"keys": []map[string]any{
					{
						"kty": "RSA",
						"alg": "RS256",
						"use": "sig",
						"kid": kid,
						"n":   base64.RawURLEncoding.EncodeToString(nBytes),
						"e":   base64.RawURLEncoding.EncodeToString(eBytes),
					},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer oidcServer.Close()
	serverURL = oidcServer.URL

	auth, err := NewRemoteOidcAuthenticator(
		serverURL,
		[]string{"https://alias.issuer.example"},
		"openfga-audience",
		[]string{"service-account-1"},
		[]string{"custom_client_id", "azp"},
	)
	require.NoError(t, err)
	defer auth.Close()

	now := time.Now()
	validToken := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss":              "https://alias.issuer.example",
		"aud":              "openfga-audience",
		"sub":              "service-account-1",
		"custom_client_id": "my-client-app",
		"scope":            "read write",
		"iat":              now.Add(-time.Minute).Unix(),
		"exp":              now.Add(10 * time.Minute).Unix(),
	})
	validToken.Header["kid"] = kid
	signedValid, err := validToken.SignedString(privKey)
	require.NoError(t, err)

	ctx := withBearerToken(context.Background(), signedValid)
	claims, err := auth.Authenticate(ctx)
	require.NoError(t, err)
	require.Equal(t, "service-account-1", claims.Subject)
	require.Equal(t, "my-client-app", claims.ClientID)
	require.True(t, claims.Scopes["read"])
	require.True(t, claims.Scopes["write"])

	wrongSubject := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss": serverURL,
		"aud": "openfga-audience",
		"sub": "unauthorized-subject",
		"exp": now.Add(10 * time.Minute).Unix(),
	})
	wrongSubject.Header["kid"] = kid
	signedWrongSub, err := wrongSubject.SignedString(privKey)
	require.NoError(t, err)

	_, err = auth.Authenticate(withBearerToken(context.Background(), signedWrongSub))
	require.Error(t, err)
}

func withBearerToken(ctx context.Context, token string) context.Context {
	return WithAuthorizationHeader(ctx, "Bearer "+token)
}

func TestServerContextRejectsNonRiffDBEngine(t *testing.T) {
	cfg := serverconfig.DefaultConfig()
	cfg.Datastore.Engine = "postgres"

	srv := &ServerContext{
		Logger:    logger.NewNoopLogger(),
		Datastore: memory.New(),
	}
	err := srv.Run(context.Background(), cfg, DefaultRiffDBConfig())
	require.ErrorContains(t, err, `unsupported datastore engine "postgres"`)
}

func TestServerContextHTTPGatewayMetricsAuthAndPlayground(t *testing.T) {
	ds := memory.New()
	defer ds.Close()

	grpcAddr := freeTCPAddr(t)
	httpAddr := freeTCPAddr(t)
	metricsAddr := freeTCPAddr(t)
	playgroundAddr := freeTCPAddr(t)

	cfg := serverconfig.DefaultConfig()
	cfg.Datastore.Engine = "riffdb"
	cfg.GRPC.Addr = grpcAddr
	cfg.HTTP.Enabled = true
	cfg.HTTP.Addr = httpAddr
	cfg.HTTP.CORSAllowedOrigins = []string{"https://console.example.com"}
	cfg.HTTP.CORSAllowedHeaders = []string{"Authorization", "Content-Type"}
	cfg.Metrics.Enabled = true
	cfg.Metrics.Addr = metricsAddr
	cfg.Metrics.EnableRPCHistograms = true
	cfg.Playground.Enabled = true
	cfg.Playground.Addr = playgroundAddr
	cfg.Authn.Method = "none"
	cfg.ShutdownTimeout = 5 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := &ServerContext{
		Logger:    logger.NewNoopLogger(),
		Datastore: ds,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Run(ctx, cfg, DefaultRiffDBConfig())
	}()

	waitForHTTPReady(t, fmt.Sprintf("http://%s/healthz", httpAddr))

	// Verify CORS preflight on HTTP gateway.
	preflightReq, err := http.NewRequest(http.MethodOptions, fmt.Sprintf("http://%s/stores", httpAddr), nil)
	require.NoError(t, err)
	preflightReq.Header.Set("Origin", "https://console.example.com")
	preflightReq.Header.Set("Access-Control-Request-Method", "POST")
	preflightResp, err := http.DefaultClient.Do(preflightReq)
	require.NoError(t, err)
	require.NoError(t, preflightResp.Body.Close())
	require.Equal(t, http.StatusNoContent, preflightResp.StatusCode)
	require.Equal(t, "https://console.example.com", preflightResp.Header.Get("Access-Control-Allow-Origin"))

	// Create a store via HTTP REST gateway.
	createResp, err := http.Post(
		fmt.Sprintf("http://%s/stores", httpAddr),
		"application/json",
		bytes.NewBufferString(`{"name":"test-store"}`),
	)
	require.NoError(t, err)
	bodyBytes, err := io.ReadAll(createResp.Body)
	require.NoError(t, createResp.Body.Close())
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, createResp.StatusCode, string(bodyBytes))

	var created openfgav1.CreateStoreResponse
	require.NoError(t, protojson.Unmarshal(bodyBytes, &created))
	require.NotEmpty(t, created.GetId())

	// Verify Prometheus /metrics endpoint.
	metricsResp, err := http.Get(fmt.Sprintf("http://%s/metrics", metricsAddr))
	require.NoError(t, err)
	metricsBody, err := io.ReadAll(metricsResp.Body)
	require.NoError(t, metricsResp.Body.Close())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, metricsResp.StatusCode)
	metricsText := string(metricsBody)
	require.Contains(t, metricsText, "openfga_build_info")
	require.Contains(t, metricsText, "grpc_server_handled_total")
	require.Contains(t, metricsText, "grpc_server_handling_seconds_bucket")

	// Verify OpenFGA Playground endpoint.
	playgroundResp, err := http.Get(fmt.Sprintf("http://%s/playground", playgroundAddr))
	require.NoError(t, err)
	playgroundBody, err := io.ReadAll(playgroundResp.Body)
	require.NoError(t, playgroundResp.Body.Close())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, playgroundResp.StatusCode)
	require.Contains(t, string(playgroundBody), "https://play.fga.dev/sandbox/")

	cancel()
	require.NoError(t, <-errCh)
}

func TestValidateModels(t *testing.T) {
	ctx := context.Background()
	ds := memory.New()
	defer ds.Close()

	store, err := ds.CreateStore(ctx, &openfgav1.Store{Id: "01HTESTSTORE00000000000001", Name: "validate-test"})
	require.NoError(t, err)

	validModel := &openfgav1.AuthorizationModel{
		Id:            "01HTESTMODEL00000000000001",
		SchemaVersion: "1.1",
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{Type: "user"},
		},
	}
	require.NoError(t, ds.WriteAuthorizationModel(ctx, store.GetId(), validModel))

	results, err := ValidateModels(ctx, logger.NewNoopLogger(), ds)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, store.GetId(), results[0].StoreID)
	require.Equal(t, validModel.GetId(), results[0].ModelID)
	require.False(t, results[0].Invalid)
}

func waitForHTTPReady(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s to become ready", url)
}
