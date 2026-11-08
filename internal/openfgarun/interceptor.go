package openfgarun

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/authclaims"
	"github.com/openfga/openfga/pkg/gateway"
	"github.com/openfga/openfga/pkg/logger"
	httpmiddleware "github.com/openfga/openfga/pkg/middleware/http"
	"github.com/openfga/openfga/pkg/middleware/requestid"
	"github.com/openfga/openfga/pkg/middleware/storeid"
	"github.com/openfga/openfga/pkg/server"
	servererrors "github.com/openfga/openfga/pkg/server/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	openfgaServiceName = "openfga.v1.OpenFGAService"
	authzenServiceName = "authzen.v1.AuthZenService"
)

type validatableRequest interface {
	Validate() error
}

type storeIDRequest interface {
	GetStoreId() string
}

// interceptedServer wraps *server.Server with OpenFGA's authentication,
// request validation, request-id/store-id tagging, timeout, panic recovery,
// structured logging, OTel tracing, and Prometheus RPC metrics without
// importing raw gRPC transport packages in the application repository.
type interceptedServer struct {
	openfgav1.UnimplementedOpenFGAServiceServer
	authzenv1.UnimplementedAuthZenServiceServer

	inner          *server.Server
	transport      gateway.Transport
	authenticator  Authenticator
	metrics        *grpcServerMetrics
	logger         logger.Logger
	requestTimeout time.Duration
	traceEnabled   bool
}

var (
	_ openfgav1.OpenFGAServiceServer = (*interceptedServer)(nil)
	_ authzenv1.AuthZenServiceServer = (*interceptedServer)(nil)
)

func invokeUnary[Req any, Resp any](
	s *interceptedServer,
	ctx context.Context,
	serviceName string,
	methodName string,
	req Req,
	fn func(context.Context, Req) (Resp, error),
) (resp Resp, err error) {
	start := time.Now()

	if s.traceEnabled {
		var span trace.Span
		ctx, span = otel.Tracer("openfga/grpc").Start(
			ctx,
			serviceName+"/"+methodName,
			trace.WithSpanKind(trace.SpanKindServer),
		)
		defer span.End()
	}

	reqID := requestid.InitRequestID(ctx)
	tags := grpc_ctxtags.NewTags()
	tags.Set("request_id", reqID)
	ctx = grpc_ctxtags.SetInContext(ctx, tags)
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("request_id", reqID))
	if s.transport != nil {
		s.transport.SetHeader(ctx, requestid.RequestIDHeader, reqID)
	}

	if sr, ok := any(req).(storeIDRequest); ok {
		if sid := sr.GetStoreId(); sid != "" {
			tags.Set("store_id", sid)
			trace.SpanFromContext(ctx).SetAttributes(attribute.String("store_id", sid))
			if s.transport != nil {
				s.transport.SetHeader(ctx, storeid.StoreIDHeader, sid)
			}
		}
	}

	defer func() {
		if r := recover(); r != nil {
			s.logger.ErrorWithContext(
				ctx,
				"panic recovered",
				zap.Any("error", r),
				zap.String("grpc_service", serviceName),
				zap.String("grpc_method", methodName),
			)
			err = servererrors.NewInternalError("", fmt.Errorf("panic: %v", r))
		}
		sent := 0
		if err == nil {
			sent = 1
		}
		codeLabel := grpcCodeLabel(err)
		s.metrics.observeRPC("unary", serviceName, methodName, codeLabel, time.Since(start), sent)
	}()

	if s.requestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.requestTimeout)
		defer cancel()
	}

	if s.authenticator != nil {
		claims, authErr := s.authenticator.Authenticate(ctx)
		if authErr != nil {
			return resp, toEncodedError(authErr)
		}
		ctx = authclaims.ContextWithAuthClaims(ctx, claims)
	}

	if vr, ok := any(req).(validatableRequest); ok {
		if vErr := vr.Validate(); vErr != nil {
			return resp, toValidationEncodedError(vErr)
		}
	}

	resp, err = fn(ctx, req)
	if err != nil {
		return resp, toEncodedError(err)
	}
	return resp, nil
}

func (s *interceptedServer) Read(ctx context.Context, req *openfgav1.ReadRequest) (*openfgav1.ReadResponse, error) {
	return invokeUnary(s, ctx, openfgaServiceName, "Read", req, s.inner.Read)
}

func (s *interceptedServer) Write(ctx context.Context, req *openfgav1.WriteRequest) (*openfgav1.WriteResponse, error) {
	return invokeUnary(s, ctx, openfgaServiceName, "Write", req, s.inner.Write)
}

func (s *interceptedServer) Check(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
	return invokeUnary(s, ctx, openfgaServiceName, "Check", req, s.inner.Check)
}

func (s *interceptedServer) BatchCheck(ctx context.Context, req *openfgav1.BatchCheckRequest) (*openfgav1.BatchCheckResponse, error) {
	return invokeUnary(s, ctx, openfgaServiceName, "BatchCheck", req, s.inner.BatchCheck)
}

func (s *interceptedServer) Expand(ctx context.Context, req *openfgav1.ExpandRequest) (*openfgav1.ExpandResponse, error) {
	return invokeUnary(s, ctx, openfgaServiceName, "Expand", req, s.inner.Expand)
}

func (s *interceptedServer) ReadAuthorizationModels(ctx context.Context, req *openfgav1.ReadAuthorizationModelsRequest) (*openfgav1.ReadAuthorizationModelsResponse, error) {
	return invokeUnary(s, ctx, openfgaServiceName, "ReadAuthorizationModels", req, s.inner.ReadAuthorizationModels)
}

func (s *interceptedServer) ReadAuthorizationModel(ctx context.Context, req *openfgav1.ReadAuthorizationModelRequest) (*openfgav1.ReadAuthorizationModelResponse, error) {
	return invokeUnary(s, ctx, openfgaServiceName, "ReadAuthorizationModel", req, s.inner.ReadAuthorizationModel)
}

func (s *interceptedServer) WriteAuthorizationModel(ctx context.Context, req *openfgav1.WriteAuthorizationModelRequest) (*openfgav1.WriteAuthorizationModelResponse, error) {
	return invokeUnary(s, ctx, openfgaServiceName, "WriteAuthorizationModel", req, s.inner.WriteAuthorizationModel)
}

func (s *interceptedServer) WriteAssertions(ctx context.Context, req *openfgav1.WriteAssertionsRequest) (*openfgav1.WriteAssertionsResponse, error) {
	return invokeUnary(s, ctx, openfgaServiceName, "WriteAssertions", req, s.inner.WriteAssertions)
}

func (s *interceptedServer) ReadAssertions(ctx context.Context, req *openfgav1.ReadAssertionsRequest) (*openfgav1.ReadAssertionsResponse, error) {
	return invokeUnary(s, ctx, openfgaServiceName, "ReadAssertions", req, s.inner.ReadAssertions)
}

func (s *interceptedServer) ReadChanges(ctx context.Context, req *openfgav1.ReadChangesRequest) (*openfgav1.ReadChangesResponse, error) {
	return invokeUnary(s, ctx, openfgaServiceName, "ReadChanges", req, s.inner.ReadChanges)
}

func (s *interceptedServer) CreateStore(ctx context.Context, req *openfgav1.CreateStoreRequest) (*openfgav1.CreateStoreResponse, error) {
	return invokeUnary(s, ctx, openfgaServiceName, "CreateStore", req, s.inner.CreateStore)
}

func (s *interceptedServer) UpdateStore(ctx context.Context, req *openfgav1.UpdateStoreRequest) (*openfgav1.UpdateStoreResponse, error) {
	return invokeUnary(s, ctx, openfgaServiceName, "UpdateStore", req, s.inner.UpdateStore)
}

func (s *interceptedServer) DeleteStore(ctx context.Context, req *openfgav1.DeleteStoreRequest) (*openfgav1.DeleteStoreResponse, error) {
	return invokeUnary(s, ctx, openfgaServiceName, "DeleteStore", req, s.inner.DeleteStore)
}

func (s *interceptedServer) GetStore(ctx context.Context, req *openfgav1.GetStoreRequest) (*openfgav1.GetStoreResponse, error) {
	return invokeUnary(s, ctx, openfgaServiceName, "GetStore", req, s.inner.GetStore)
}

func (s *interceptedServer) ListStores(ctx context.Context, req *openfgav1.ListStoresRequest) (*openfgav1.ListStoresResponse, error) {
	return invokeUnary(s, ctx, openfgaServiceName, "ListStores", req, s.inner.ListStores)
}

func (s *interceptedServer) ListObjects(ctx context.Context, req *openfgav1.ListObjectsRequest) (*openfgav1.ListObjectsResponse, error) {
	return invokeUnary(s, ctx, openfgaServiceName, "ListObjects", req, s.inner.ListObjects)
}

func (s *interceptedServer) ListUsers(ctx context.Context, req *openfgav1.ListUsersRequest) (*openfgav1.ListUsersResponse, error) {
	return invokeUnary(s, ctx, openfgaServiceName, "ListUsers", req, s.inner.ListUsers)
}

type streamedListObjectsStream struct {
	openfgav1.OpenFGAService_StreamedListObjectsServer
	ctx   context.Context
	send  func(*openfgav1.StreamedListObjectsResponse) error
	count int
}

func (s *streamedListObjectsStream) Context() context.Context {
	return s.ctx
}

func (s *streamedListObjectsStream) Send(resp *openfgav1.StreamedListObjectsResponse) error {
	if err := s.send(resp); err != nil {
		return err
	}
	s.count++
	return nil
}

func (s *interceptedServer) StreamedListObjects(
	req *openfgav1.StreamedListObjectsRequest,
	srv openfgav1.OpenFGAService_StreamedListObjectsServer,
) (err error) {
	start := time.Now()
	ctx := srv.Context()

	if s.traceEnabled {
		var span trace.Span
		ctx, span = otel.Tracer("openfga/grpc").Start(
			ctx,
			openfgaServiceName+"/StreamedListObjects",
			trace.WithSpanKind(trace.SpanKindServer),
		)
		defer span.End()
	}

	reqID := requestid.InitRequestID(ctx)
	tags := grpc_ctxtags.NewTags()
	tags.Set("request_id", reqID)
	if sid := req.GetStoreId(); sid != "" {
		tags.Set("store_id", sid)
	}
	ctx = grpc_ctxtags.SetInContext(ctx, tags)

	wrapped := &streamedListObjectsStream{
		OpenFGAService_StreamedListObjectsServer: srv,
		ctx:                                      ctx,
		send:                                     srv.Send,
	}

	defer func() {
		if r := recover(); r != nil {
			s.logger.ErrorWithContext(ctx, "panic recovered in StreamedListObjects", zap.Any("error", r))
			err = servererrors.NewInternalError("", fmt.Errorf("panic: %v", r))
		}
		codeLabel := grpcCodeLabel(err)
		s.metrics.observeRPC("server_stream", openfgaServiceName, "StreamedListObjects", codeLabel, time.Since(start), wrapped.count)
	}()

	if s.requestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.requestTimeout)
		defer cancel()
		wrapped.ctx = ctx
	}

	if s.authenticator != nil {
		claims, authErr := s.authenticator.Authenticate(ctx)
		if authErr != nil {
			return toEncodedError(authErr)
		}
		wrapped.ctx = authclaims.ContextWithAuthClaims(ctx, claims)
	}

	if vErr := req.Validate(); vErr != nil {
		return toValidationEncodedError(vErr)
	}

	if err := s.inner.StreamedListObjects(req, wrapped); err != nil {
		return toEncodedError(err)
	}
	return nil
}

func (s *interceptedServer) Evaluation(ctx context.Context, req *authzenv1.EvaluationRequest) (*authzenv1.EvaluationResponse, error) {
	return invokeUnary(s, ctx, authzenServiceName, "Evaluation", req, s.inner.Evaluation)
}

func (s *interceptedServer) Evaluations(ctx context.Context, req *authzenv1.EvaluationsRequest) (*authzenv1.EvaluationsResponse, error) {
	return invokeUnary(s, ctx, authzenServiceName, "Evaluations", req, s.inner.Evaluations)
}

func (s *interceptedServer) SubjectSearch(ctx context.Context, req *authzenv1.SubjectSearchRequest) (*authzenv1.SubjectSearchResponse, error) {
	return invokeUnary(s, ctx, authzenServiceName, "SubjectSearch", req, s.inner.SubjectSearch)
}

func (s *interceptedServer) ResourceSearch(ctx context.Context, req *authzenv1.ResourceSearchRequest) (*authzenv1.ResourceSearchResponse, error) {
	return invokeUnary(s, ctx, authzenServiceName, "ResourceSearch", req, s.inner.ResourceSearch)
}

func (s *interceptedServer) ActionSearch(ctx context.Context, req *authzenv1.ActionSearchRequest) (*authzenv1.ActionSearchResponse, error) {
	return invokeUnary(s, ctx, authzenServiceName, "ActionSearch", req, s.inner.ActionSearch)
}

func (s *interceptedServer) GetConfiguration(ctx context.Context, req *authzenv1.GetConfigurationRequest) (*authzenv1.GetConfigurationResponse, error) {
	return invokeUnary(s, ctx, authzenServiceName, "GetConfiguration", req, s.inner.GetConfiguration)
}

func grpcCodeLabel(err error) string {
	if err == nil {
		return "OK"
	}
	var enc *servererrors.EncodedError
	if errors.As(err, &enc) {
		return fmt.Sprintf("Code(%d)", enc.CodeValue())
	}
	codeInt, _ := parseStatusFromErrorString(err.Error())
	if codeInt >= 1000 {
		return fmt.Sprintf("Code(%d)", codeInt)
	}
	return standardCodeName(codeInt)
}

func standardCodeName(code int32) string {
	switch code {
	case 0:
		return "OK"
	case 1:
		return "Canceled"
	case 2:
		return "Unknown"
	case 3:
		return "InvalidArgument"
	case 4:
		return "DeadlineExceeded"
	case 5:
		return "NotFound"
	case 6:
		return "AlreadyExists"
	case 7:
		return "PermissionDenied"
	case 8:
		return "ResourceExhausted"
	case 9:
		return "FailedPrecondition"
	case 10:
		return "Aborted"
	case 11:
		return "OutOfRange"
	case 12:
		return "Unimplemented"
	case 13:
		return "Internal"
	case 14:
		return "Unavailable"
	case 15:
		return "DataLoss"
	case 16:
		return "Unauthenticated"
	default:
		return fmt.Sprintf("Code(%d)", code)
	}
}

func toValidationEncodedError(err error) *servererrors.EncodedError {
	if err == nil {
		return nil
	}
	var enc *servererrors.EncodedError
	if errors.As(err, &enc) {
		return enc
	}
	msg := err.Error()
	code := customizeValidationErrorCode(msg)
	return servererrors.NewEncodedError(code, msg)
}

func toEncodedError(err error) *servererrors.EncodedError {
	if err == nil {
		return nil
	}
	var enc *servererrors.EncodedError
	if errors.As(err, &enc) {
		return enc
	}
	codeInt, msg := parseStatusFromErrorString(err.Error())
	return servererrors.NewEncodedError(codeInt, msg)
}

func parseStatusFromErrorString(raw string) (int32, string) {
	const prefix = "rpc error: code = "
	if strings.HasPrefix(raw, prefix) {
		rest := strings.TrimPrefix(raw, prefix)
		codePart, desc, found := strings.Cut(rest, " desc = ")
		if !found {
			desc = raw
		}
		if strings.HasPrefix(codePart, "Code(") && strings.HasSuffix(codePart, ")") {
			numStr := strings.TrimSuffix(strings.TrimPrefix(codePart, "Code("), ")")
			if n, err := strconv.ParseInt(numStr, 10, 32); err == nil {
				return int32(n), desc
			}
		}
		switch codePart {
		case "Unauthenticated":
			return int32(openfgav1.AuthErrorCode_unauthenticated), desc
		case "Canceled":
			return int32(openfgav1.ErrorCode_cancelled), desc
		case "DeadlineExceeded":
			return int32(openfgav1.InternalErrorCode_deadline_exceeded), desc
		case "NotFound":
			return int32(openfgav1.NotFoundErrorCode_undefined_endpoint), desc
		case "AlreadyExists":
			return int32(openfgav1.InternalErrorCode_already_exists), desc
		case "ResourceExhausted":
			return int32(openfgav1.InternalErrorCode_resource_exhausted), desc
		case "FailedPrecondition":
			return int32(openfgav1.InternalErrorCode_failed_precondition), desc
		case "Aborted":
			return 10, desc
		case "OutOfRange":
			return int32(openfgav1.InternalErrorCode_out_of_range), desc
		case "Unimplemented":
			return int32(openfgav1.NotFoundErrorCode_unimplemented), desc
		case "Unavailable":
			return int32(openfgav1.InternalErrorCode_unavailable), desc
		case "DataLoss":
			return int32(openfgav1.InternalErrorCode_data_loss), desc
		case "InvalidArgument":
			return customizeValidationErrorCode(desc), desc
		default:
			return int32(openfgav1.InternalErrorCode_internal_error), desc
		}
	}
	return customizeValidationErrorCode(raw), raw
}

func customizeValidationErrorCode(message string) int32 {
	parsedMessages := strings.Split(message, "| caused by:")
	lastMessage := strings.TrimSpace(parsedMessages[len(parsedMessages)-1])
	lastMessage = strings.TrimSpace(strings.TrimPrefix(lastMessage, "proto:"))

	parts := strings.SplitN(lastMessage, ": ", 2)
	if len(parts) < 2 {
		return int32(openfgav1.ErrorCode_validation_error)
	}
	objParts := strings.Split(parts[0], ".")
	if len(objParts) != 2 {
		return int32(openfgav1.ErrorCode_validation_error)
	}
	field, reason := objParts[1], parts[1]
	switch field {
	case "Assertions":
		if strings.HasPrefix(reason, "value must contain no more than") {
			return int32(openfgav1.ErrorCode_assertions_too_many_items)
		}
	case "AuthorizationModelId":
		if strings.HasPrefix(reason, "value length must be at most") {
			return int32(openfgav1.ErrorCode_authorization_model_id_too_long)
		}
	case "Base":
		if strings.HasPrefix(reason, "value is required") {
			return int32(openfgav1.ErrorCode_difference_base_missing_value)
		}
	case "Id":
		if strings.HasPrefix(reason, "value length must be at most") {
			return int32(openfgav1.ErrorCode_id_too_long)
		}
	case "Object":
		if strings.HasPrefix(reason, "value length must be at most") {
			return int32(openfgav1.ErrorCode_object_too_long)
		}
	case "PageSize":
		if strings.HasPrefix(reason, "value must be inside range") {
			return int32(openfgav1.ErrorCode_page_size_invalid)
		}
	case "Params":
		if strings.HasPrefix(reason, "value is required") {
			return int32(openfgav1.ErrorCode_param_missing_value)
		}
	case "Relation":
		if strings.HasPrefix(reason, "value length must be at most") {
			return int32(openfgav1.ErrorCode_relation_too_long)
		}
	case "Relations":
		if strings.HasPrefix(reason, "value must contain at least") {
			return int32(openfgav1.ErrorCode_relations_too_few_items)
		}
	case "Subtract":
		if strings.HasPrefix(reason, "value is required") {
			return int32(openfgav1.ErrorCode_subtract_base_missing_value)
		}
	case "StoreId":
		if strings.HasPrefix(reason, "value length must be") {
			return int32(openfgav1.ErrorCode_store_id_invalid_length)
		}
	case "TupleKey":
		if strings.HasPrefix(reason, "value is required") {
			return int32(openfgav1.ErrorCode_tuple_key_value_not_specified)
		}
	case "TupleKeys":
		if strings.HasPrefix(reason, "value must contain between") {
			return int32(openfgav1.ErrorCode_tuple_keys_too_many_or_too_few_items)
		}
	case "Type":
		if strings.HasPrefix(reason, "value length must be at") {
			return int32(openfgav1.ErrorCode_type_invalid_length)
		}
		if strings.HasPrefix(reason, "value does not match regex pattern") {
			return int32(openfgav1.ErrorCode_type_invalid_pattern)
		}
	case "TypeDefinitions":
		if strings.HasPrefix(reason, "value must contain at least") {
			return int32(openfgav1.ErrorCode_type_definitions_too_few_items)
		}
	}
	if strings.HasPrefix(field, "Relations[") {
		if strings.HasPrefix(reason, "value length must be at most") {
			return int32(openfgav1.ErrorCode_relations_too_long)
		}
		if strings.HasPrefix(reason, "value does not match regex pattern") {
			return int32(openfgav1.ErrorCode_relations_invalid_pattern)
		}
	}
	return int32(openfgav1.ErrorCode_validation_error)
}

func wrapGatewayHTTPHandler(
	gwMux *gwruntime.ServeMux,
	srv *interceptedServer,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if authHeader := r.Header.Get("Authorization"); authHeader != "" {
			r = r.WithContext(WithAuthorizationHeader(r.Context(), authHeader))
		}

		if r.Method == http.MethodGet && r.URL.Path == "/healthz" {
			ready, err := srv.inner.IsReady(r.Context())
			w.Header().Set("Content-Type", "application/json")
			if err != nil || !ready {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"status":"NOT_SERVING"}`))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"SERVING"}`))
			return
		}

		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/stores/") && strings.HasSuffix(r.URL.Path, "/streamed-list-objects") {
			serveHTTPStreamedListObjects(w, r, gwMux, srv)
			return
		}

		gwMux.ServeHTTP(w, r)
	})
}

func serveHTTPStreamedListObjects(
	w http.ResponseWriter,
	r *http.Request,
	gwMux *gwruntime.ServeMux,
	srv *interceptedServer,
) {
	trimmed := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/stores/"), "/streamed-list-objects")
	if trimmed == "" || strings.Contains(trimmed, "/") {
		httpmiddleware.CustomHTTPErrorHandler(
			r.Context(),
			w,
			r,
			servererrors.NewEncodedError(int32(openfgav1.NotFoundErrorCode_undefined_endpoint), "undefined endpoint"),
		)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		httpmiddleware.CustomHTTPErrorHandler(
			r.Context(),
			w,
			r,
			servererrors.NewEncodedError(int32(openfgav1.ErrorCode_validation_error), err.Error()),
		)
		return
	}

	var req openfgav1.StreamedListObjectsRequest
	if len(bytes.TrimSpace(body)) > 0 {
		if err := protojson.Unmarshal(body, &req); err != nil {
			httpmiddleware.CustomHTTPErrorHandler(
				r.Context(),
				w,
				r,
				servererrors.NewEncodedError(int32(openfgav1.ErrorCode_validation_error), err.Error()),
			)
			return
		}
	}
	req.StoreId = trimmed

	annotatedCtx, err := gwruntime.AnnotateIncomingContext(
		r.Context(),
		gwMux,
		r,
		"/openfga.v1.OpenFGAService/StreamedListObjects",
		gwruntime.WithHTTPPathPattern("/stores/{store_id}/streamed-list-objects"),
	)
	if err != nil {
		httpmiddleware.CustomHTTPErrorHandler(r.Context(), w, r, toEncodedError(err))
		return
	}

	wroteHeader := false
	stream := &streamedListObjectsStream{
		ctx: annotatedCtx,
		send: func(resp *openfgav1.StreamedListObjectsResponse) error {
			if !wroteHeader {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Transfer-Encoding", "chunked")
				w.WriteHeader(http.StatusOK)
				wroteHeader = true
			}
			payload, err := protojson.Marshal(resp)
			if err != nil {
				return err
			}
			var envelope struct {
				Result json.RawMessage `json:"result"`
			}
			envelope.Result = payload
			line, err := json.Marshal(envelope)
			if err != nil {
				return err
			}
			line = append(line, '\n')
			if _, err := w.Write(line); err != nil {
				return err
			}
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			return nil
		},
	}

	if err := srv.StreamedListObjects(&req, stream); err != nil {
		if !wroteHeader {
			httpmiddleware.CustomHTTPErrorHandler(annotatedCtx, w, r, toEncodedError(err))
			return
		}
		enc := toEncodedError(err)
		errEnvelope, _ := json.Marshal(map[string]any{
			"error": map[string]any{
				"code":    enc.Code(),
				"message": enc.Error(),
			},
		})
		_, _ = w.Write(append(errEnvelope, '\n'))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		return
	}

	if !wroteHeader {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
	}
}

// newGRPCProtocolHandler serves native HTTP/2 gRPC frames for
// OpenFGAService, AuthZenService, and grpc.health.v1.Health on the gRPC port.
func newGRPCProtocolHandler(srv *interceptedServer, maxRecvBytes int, fallback http.Handler) http.Handler {
	if maxRecvBytes <= 0 {
		maxRecvBytes = 4 << 20
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contentType := r.Header.Get("Content-Type")
		if r.Method == http.MethodPost && strings.HasPrefix(contentType, "application/grpc") {
			serveGRPCFrame(w, r, srv, maxRecvBytes)
			return
		}
		if fallback != nil {
			fallback.ServeHTTP(w, r)
			return
		}
		http.NotFound(w, r)
	})
}

func serveGRPCFrame(w http.ResponseWriter, r *http.Request, srv *interceptedServer, maxRecvBytes int) {
	w.Header().Set("Content-Type", "application/grpc")
	w.Header().Set("Trailer", "Grpc-Status, Grpc-Message")

	ctx := r.Context()
	if authHeader := r.Header.Get("Authorization"); authHeader != "" {
		ctx = WithAuthorizationHeader(ctx, authHeader)
	}

	payload, statusCode, statusMsg := readGRPCMessageFrame(r.Body, maxRecvBytes)
	if statusCode != 0 {
		writeGRPCTrailers(w, statusCode, statusMsg)
		return
	}

	if r.URL.Path == "/grpc.health.v1.Health/Check" {
		ready, err := srv.inner.IsReady(ctx)
		if err != nil {
			writeGRPCTrailers(w, 13, err.Error())
			return
		}
		statusVal := byte(1) // SERVING
		if !ready {
			statusVal = 2 // NOT_SERVING
		}
		// Protobuf wire encoding for HealthCheckResponse { Status status = 1; }: tag 0x08, varint statusVal.
		writeGRPCMessageFrame(w, []byte{0x08, statusVal})
		writeGRPCTrailers(w, 0, "")
		return
	}

	if r.URL.Path == openfgav1.OpenFGAService_StreamedListObjects_FullMethodName {
		var req openfgav1.StreamedListObjectsRequest
		if err := proto.Unmarshal(payload, &req); err != nil {
			writeGRPCTrailers(w, 3, err.Error())
			return
		}
		w.WriteHeader(http.StatusOK)
		stream := &streamedListObjectsStream{
			ctx: ctx,
			send: func(resp *openfgav1.StreamedListObjectsResponse) error {
				outBytes, err := proto.Marshal(resp)
				if err != nil {
					return err
				}
				writeGRPCMessageFrame(w, outBytes)
				if flusher, ok := w.(http.Flusher); ok {
					flusher.Flush()
				}
				return nil
			},
		}
		if err := srv.StreamedListObjects(&req, stream); err != nil {
			enc := toEncodedError(err)
			writeGRPCTrailers(w, enc.CodeValue(), enc.Error())
			return
		}
		writeGRPCTrailers(w, 0, "")
		return
	}

	respMsg, err := dispatchUnaryGRPC(ctx, srv, r.URL.Path, payload)
	if err != nil {
		enc := toEncodedError(err)
		w.WriteHeader(http.StatusOK)
		writeGRPCTrailers(w, enc.CodeValue(), enc.Error())
		return
	}

	outBytes, err := proto.Marshal(respMsg)
	if err != nil {
		w.WriteHeader(http.StatusOK)
		writeGRPCTrailers(w, 13, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
	writeGRPCMessageFrame(w, outBytes)
	writeGRPCTrailers(w, 0, "")
}

func dispatchUnaryGRPC(ctx context.Context, srv *interceptedServer, fullMethod string, payload []byte) (proto.Message, error) {
	dec := func(in any) error {
		msg, ok := in.(proto.Message)
		if !ok {
			return errors.New("invalid protobuf target")
		}
		return proto.Unmarshal(payload, msg)
	}

	const openfgaPrefix = "/" + openfgaServiceName + "/"
	if strings.HasPrefix(fullMethod, openfgaPrefix) {
		methodName := strings.TrimPrefix(fullMethod, openfgaPrefix)
		for _, md := range openfgav1.OpenFGAService_ServiceDesc.Methods {
			if md.MethodName == methodName {
				resp, err := md.Handler(srv, ctx, dec, nil)
				if err != nil {
					return nil, err
				}
				return resp.(proto.Message), nil
			}
		}
	}

	const authzenPrefix = "/" + authzenServiceName + "/"
	if strings.HasPrefix(fullMethod, authzenPrefix) {
		methodName := strings.TrimPrefix(fullMethod, authzenPrefix)
		for _, md := range authzenv1.AuthZenService_ServiceDesc.Methods {
			if md.MethodName == methodName {
				resp, err := md.Handler(srv, ctx, dec, nil)
				if err != nil {
					return nil, err
				}
				return resp.(proto.Message), nil
			}
		}
	}

	return nil, servererrors.NewEncodedError(int32(openfgav1.NotFoundErrorCode_unimplemented), "unimplemented method "+fullMethod)
}

func readGRPCMessageFrame(r io.Reader, maxRecvBytes int) ([]byte, int32, string) {
	var header [5]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, 0, ""
		}
		return nil, 13, err.Error()
	}
	if header[0] != 0 {
		return nil, 12, "compressed gRPC frames are not supported"
	}
	msgLen := int(binary.BigEndian.Uint32(header[1:5]))
	if msgLen > maxRecvBytes {
		return nil, 8, fmt.Sprintf("gRPC message size %d exceeds maximum %d", msgLen, maxRecvBytes)
	}
	payload := make([]byte, msgLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, 13, err.Error()
	}
	return payload, 0, ""
}

func writeGRPCMessageFrame(w http.ResponseWriter, payload []byte) {
	var header [5]byte
	header[0] = 0
	binary.BigEndian.PutUint32(header[1:5], uint32(len(payload)))
	_, _ = w.Write(header[:])
	if len(payload) > 0 {
		_, _ = w.Write(payload)
	}
}

func writeGRPCTrailers(w http.ResponseWriter, code int32, message string) {
	w.Header().Set("Grpc-Status", strconv.FormatInt(int64(code), 10))
	if message != "" {
		w.Header().Set("Grpc-Message", url.PathEscape(message))
	}
}
