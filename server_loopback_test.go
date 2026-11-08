package riffdbopenfga_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/logger"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	riffdbopenfga "github.com/riffdb/riffdb-openfga"
	"github.com/riffdb/riffdb-openfga/internal/openfgarun"
)

func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())
	return addr
}

func TestLoopbackConformanceServerEndToEnd(t *testing.T) {
	if os.Getenv("RIFFDB_LOOPBACK_SOCKET") == "" && os.Getenv("RIFFDB_DRIVER_SOCKET") == "" {
		t.Skip("run through riffdb dev with the loopback test shim")
	}

	grpcAddr := freeLoopbackAddr(t)
	httpAddr := freeLoopbackAddr(t)
	metricsAddr := freeLoopbackAddr(t)
	const presharedKey = "riffdb-loopback-secret-key"

	cfg := serverconfig.DefaultConfig()
	cfg.Datastore.Engine = "riffdb"
	cfg.GRPC.Addr = grpcAddr
	cfg.HTTP.Enabled = true
	cfg.HTTP.Addr = httpAddr
	cfg.Playground.Enabled = false
	cfg.Metrics.Enabled = true
	cfg.Metrics.Addr = metricsAddr
	cfg.Metrics.EnableRPCHistograms = true
	cfg.Authn.Method = "preshared"
	cfg.Authn.AuthnPresharedKeyConfig = &serverconfig.AuthnPresharedKeyConfig{
		Keys: []string{presharedKey},
	}
	cfg.ShutdownTimeout = 5 * time.Second

	riffCfg := openfgarun.DefaultRiffDBConfig()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := &openfgarun.ServerContext{
		Logger: logger.NewNoopLogger(),
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Run(ctx, cfg, riffCfg)
	}()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("http://%s/healthz", httpAddr))
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		time.Sleep(25 * time.Millisecond)
	}

	// 1. Unauthenticated HTTP request must fail with 401.
	unauthResp, err := http.Post(
		fmt.Sprintf("http://%s/stores", httpAddr),
		"application/json",
		bytes.NewBufferString(`{"name":"unauth-store"}`),
	)
	require.NoError(t, err)
	_ = unauthResp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, unauthResp.StatusCode)

	// 2. Authenticated HTTP request creates a store in RiffDB.
	createBody := doAuthenticatedJSONPost(
		t,
		fmt.Sprintf("http://%s/stores", httpAddr),
		presharedKey,
		`{"name":"riffdb-e2e-store"}`,
		http.StatusCreated,
	)

	var storeResp openfgav1.CreateStoreResponse
	require.NoError(t, protojson.Unmarshal(createBody, &storeResp))
	storeID := storeResp.GetId()
	require.NotEmpty(t, storeID)

	// 3. WriteAuthorizationModel, Write, and Check via HTTP REST gateway and gRPC listener.
	modelReq := &openfgav1.WriteAuthorizationModelRequest{
		StoreId:       storeID,
		SchemaVersion: "1.1",
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{Type: "user"},
			{
				Type: "document",
				Relations: map[string]*openfgav1.Userset{
					"viewer": {
						Userset: &openfgav1.Userset_This{},
					},
				},
				Metadata: &openfgav1.Metadata{
					Relations: map[string]*openfgav1.RelationMetadata{
						"viewer": {
							DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
								{Type: "user"},
							},
						},
					},
				},
			},
		},
	}
	modelReqJSON, err := protojson.Marshal(modelReq)
	require.NoError(t, err)
	modelRespBody := doAuthenticatedJSONPost(
		t,
		fmt.Sprintf("http://%s/stores/%s/authorization-models", httpAddr, storeID),
		presharedKey,
		string(modelReqJSON),
		http.StatusCreated,
	)
	var modelResp openfgav1.WriteAuthorizationModelResponse
	require.NoError(t, protojson.Unmarshal(modelRespBody, &modelResp))
	require.NotEmpty(t, modelResp.GetAuthorizationModelId())

	writeReq := &openfgav1.WriteRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelResp.GetAuthorizationModelId(),
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				{
					User:     "user:anne",
					Relation: "viewer",
					Object:   "document:roadmap",
				},
			},
		},
	}
	writeReqJSON, err := protojson.Marshal(writeReq)
	require.NoError(t, err)
	_ = doAuthenticatedJSONPost(
		t,
		fmt.Sprintf("http://%s/stores/%s/write", httpAddr, storeID),
		presharedKey,
		string(writeReqJSON),
		http.StatusOK,
	)

	// Invoke Check over the gRPC wire protocol on grpcAddr.
	checkReq := &openfgav1.CheckRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelResp.GetAuthorizationModelId(),
		TupleKey: &openfgav1.CheckRequestTupleKey{
			User:     "user:anne",
			Relation: "viewer",
			Object:   "document:roadmap",
		},
	}
	checkReqBytes, err := proto.Marshal(checkReq)
	require.NoError(t, err)
	var frame bytes.Buffer
	frame.WriteByte(0)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(checkReqBytes)))
	frame.Write(lenBuf[:])
	frame.Write(checkReqBytes)

	grpcHTTPReq, err := http.NewRequest(
		http.MethodPost,
		fmt.Sprintf("http://%s/openfga.v1.OpenFGAService/Check", grpcAddr),
		&frame,
	)
	require.NoError(t, err)
	grpcHTTPReq.Header.Set("Content-Type", "application/grpc")
	grpcHTTPReq.Header.Set("Authorization", "Bearer "+presharedKey)
	grpcHTTPResp, err := http.DefaultClient.Do(grpcHTTPReq)
	require.NoError(t, err)
	grpcRespBytes, err := io.ReadAll(grpcHTTPResp.Body)
	require.NoError(t, grpcHTTPResp.Body.Close())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, grpcHTTPResp.StatusCode)
	require.GreaterOrEqual(t, len(grpcRespBytes), 5)

	var checkResp openfgav1.CheckResponse
	require.NoError(t, proto.Unmarshal(grpcRespBytes[5:], &checkResp))
	require.True(t, checkResp.GetAllowed())

	// 4. Verify Prometheus metrics endpoint.
	metricsResp, err := http.Get(fmt.Sprintf("http://%s/metrics", metricsAddr))
	require.NoError(t, err)
	metricsBody, err := io.ReadAll(metricsResp.Body)
	require.NoError(t, metricsResp.Body.Close())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, metricsResp.StatusCode)
	require.Contains(t, string(metricsBody), "grpc_server_handled_total")

	cancel()
	require.NoError(t, <-errCh)

	// 5. Verify ValidateModels against the real RiffDB datastore.
	session, err := riffdbopenfga.Connect(context.Background(), riffCfg.ConnectConfig)
	require.NoError(t, err)
	ds, err := riffdbopenfga.NewDatastore(riffdbopenfga.Config{
		Session:         session,
		CommandAttempts: 3,
		CloseSession:    true,
	})
	require.NoError(t, err)
	defer ds.Close()

	results, err := openfgarun.ValidateModels(context.Background(), logger.NewNoopLogger(), ds)
	require.NoError(t, err)
	var found bool
	for _, r := range results {
		if r.StoreID == storeID && r.ModelID == modelResp.GetAuthorizationModelId() {
			found = true
			require.False(t, r.Invalid, r.Error)
		}
	}
	require.True(t, found, "expected written authorization model in ValidateModels output")
}

func doAuthenticatedJSONPost(t *testing.T, url, bearerKey, body string, wantStatus int) []byte {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBufferString(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+bearerKey)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())
	require.NoError(t, err)
	require.Equal(t, wantStatus, resp.StatusCode, string(respBody))
	return respBody
}
