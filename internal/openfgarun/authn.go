package openfgarun

import (
	"context"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	"github.com/hashicorp/go-retryablehttp"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/authclaims"
	servererrors "github.com/openfga/openfga/pkg/server/errors"
)

type authHeaderContextKey struct{}

// WithAuthorizationHeader attaches a raw Authorization header value (e.g.,
// "Bearer <token>") to ctx so authenticators can inspect it without requiring
// transport-specific metadata packages.
func WithAuthorizationHeader(ctx context.Context, headerValue string) context.Context {
	return context.WithValue(ctx, authHeaderContextKey{}, headerValue)
}

func bearerTokenFromContext(ctx context.Context) (string, error) {
	if raw, ok := ctx.Value(authHeaderContextKey{}).(string); ok && raw != "" {
		scheme, token, found := strings.Cut(strings.TrimSpace(raw), " ")
		if found && strings.EqualFold(scheme, "Bearer") && strings.TrimSpace(token) != "" {
			return strings.TrimSpace(token), nil
		}
		return "", ErrMissingBearerToken
	}
	token, err := grpcauth.AuthFromMD(ctx, "Bearer")
	if err != nil || strings.TrimSpace(token) == "" {
		return "", ErrMissingBearerToken
	}
	return token, nil
}

var (
	// ErrUnauthenticated matches OpenFGA's unauthenticated status error.
	ErrUnauthenticated error = servererrors.NewEncodedError(
		int32(openfgav1.AuthErrorCode_unauthenticated),
		"unauthenticated",
	)

	// ErrMissingBearerToken matches OpenFGA's missing bearer token status error.
	ErrMissingBearerToken error = servererrors.NewEncodedError(
		int32(openfgav1.AuthErrorCode_bearer_token_missing),
		"missing bearer token",
	)

	errInvalidClaims error = servererrors.NewEncodedError(
		int32(openfgav1.AuthErrorCode_invalid_claims),
		"invalid claims",
	)

	// ErrMissingIssuer is returned when OIDC authentication is configured without an issuer.
	ErrMissingIssuer = errors.New("oidc: issuer must be set")

	// ErrMissingAudience is returned when OIDC authentication is configured without an audience.
	ErrMissingAudience = errors.New("oidc: audience must be set")
)

// Authenticator verifies incoming request credentials and returns OpenFGA AuthClaims.
type Authenticator interface {
	Authenticate(requestContext context.Context) (*authclaims.AuthClaims, error)
	Close()
}

// NoopAuthenticator permits all requests without authentication.
type NoopAuthenticator struct{}

var _ Authenticator = (*NoopAuthenticator)(nil)

// Authenticate returns empty AuthClaims for unauthenticated mode.
func (NoopAuthenticator) Authenticate(_ context.Context) (*authclaims.AuthClaims, error) {
	return &authclaims.AuthClaims{
		Subject: "",
		Scopes:  nil,
	}, nil
}

// Close is a no-op.
func (NoopAuthenticator) Close() {}

// PresharedKeyAuthenticator authenticates Bearer tokens against a set of
// pre-shared keys using constant-time SHA-256 digest comparisons.
type PresharedKeyAuthenticator struct {
	validKeyHashes [][sha256.Size]byte
}

var _ Authenticator = (*PresharedKeyAuthenticator)(nil)

// NewPresharedKeyAuthenticator constructs a PresharedKeyAuthenticator for validKeys.
func NewPresharedKeyAuthenticator(validKeys []string) (*PresharedKeyAuthenticator, error) {
	if len(validKeys) < 1 {
		return nil, errors.New("invalid auth configuration, please specify at least one key")
	}
	hashes := make([][sha256.Size]byte, 0, len(validKeys))
	for _, key := range validKeys {
		hashes = append(hashes, sha256.Sum256([]byte(key)))
	}
	return &PresharedKeyAuthenticator{validKeyHashes: hashes}, nil
}

// Authenticate validates the request's Bearer token in constant time.
func (pka *PresharedKeyAuthenticator) Authenticate(ctx context.Context) (*authclaims.AuthClaims, error) {
	authHeader, err := bearerTokenFromContext(ctx)
	if err != nil {
		return nil, ErrMissingBearerToken
	}

	tokenHash := sha256.Sum256([]byte(authHeader))
	var matched int
	for _, keyHash := range pka.validKeyHashes {
		matched |= subtle.ConstantTimeCompare(tokenHash[:], keyHash[:])
	}
	if matched == 1 {
		return &authclaims.AuthClaims{
			Subject: "",
		}, nil
	}
	return nil, ErrUnauthenticated
}

// Close is a no-op.
func (*PresharedKeyAuthenticator) Close() {}

// OidcDiscoveryConfig holds the OIDC discovery metadata needed to fetch JWKS.
type OidcDiscoveryConfig struct {
	Issuer  string `json:"issuer"`
	JWKsURI string `json:"jwks_uri"`
}

// RemoteOidcAuthenticator validates OIDC RS256 Bearer JWTs against a remote
// OIDC provider's JWKS endpoint.
type RemoteOidcAuthenticator struct {
	MainIssuer     string
	IssuerAliases  []string
	Audience       string
	Subjects       []string
	ClientIDClaims []string
	JwksURI        string

	httpClient  *http.Client
	mu          sync.RWMutex
	keys        map[string]*rsa.PublicKey
	lastRefresh time.Time
}

var _ Authenticator = (*RemoteOidcAuthenticator)(nil)

// NewRemoteOidcAuthenticator discovers the OIDC configuration and initial JWKS
// from mainIssuer and returns a configured RemoteOidcAuthenticator.
func NewRemoteOidcAuthenticator(
	mainIssuer string,
	issuerAliases []string,
	audience string,
	subjects []string,
	clientIDClaims []string,
) (*RemoteOidcAuthenticator, error) {
	if mainIssuer == "" {
		return nil, ErrMissingIssuer
	}
	if audience == "" {
		return nil, ErrMissingAudience
	}

	retryClient := retryablehttp.NewClient()
	retryClient.Logger = nil
	oidc := &RemoteOidcAuthenticator{
		MainIssuer:     mainIssuer,
		IssuerAliases:  issuerAliases,
		Audience:       audience,
		Subjects:       subjects,
		ClientIDClaims: clientIDClaims,
		httpClient:     retryClient.StandardClient(),
		keys:           make(map[string]*rsa.PublicKey),
	}
	if len(oidc.ClientIDClaims) == 0 {
		oidc.ClientIDClaims = []string{"azp", "client_id"}
	}

	cfg, err := oidc.GetConfiguration()
	if err != nil {
		return nil, fmt.Errorf("error fetching OIDC configuration: %w", err)
	}
	oidc.JwksURI = cfg.JWKsURI
	if err := oidc.refreshKeys(true); err != nil {
		return nil, fmt.Errorf("error fetching OIDC keys: %w", err)
	}
	return oidc, nil
}

// GetConfiguration fetches `.well-known/openid-configuration` from MainIssuer.
func (oidc *RemoteOidcAuthenticator) GetConfiguration() (*OidcDiscoveryConfig, error) {
	wellKnown := strings.TrimSuffix(oidc.MainIssuer, "/") + "/.well-known/openid-configuration"
	req, err := http.NewRequest(http.MethodGet, wellKnown, nil)
	if err != nil {
		return nil, fmt.Errorf("error forming request to get OIDC: %w", err)
	}

	res, err := oidc.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error getting OIDC: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code getting OIDC: %v", res.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %w", err)
	}

	oidcConfig := &OidcDiscoveryConfig{}
	if err := json.Unmarshal(body, oidcConfig); err != nil {
		return nil, fmt.Errorf("failed parsing document: %w", err)
	}
	if oidcConfig.Issuer == "" {
		return nil, errors.New("missing issuer value")
	}
	if oidcConfig.JWKsURI == "" {
		return nil, errors.New("missing jwks_uri value")
	}
	return oidcConfig, nil
}

type jwksDocument struct {
	Keys []jwkKey `json:"keys"`
}

type jwkKey struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Use string `json:"use"`
	Alg string `json:"alg"`
	N   string `json:"n"`
	E   string `json:"e"`
}

func (oidc *RemoteOidcAuthenticator) refreshKeys(force bool) error {
	oidc.mu.Lock()
	defer oidc.mu.Unlock()

	if !force && time.Since(oidc.lastRefresh) < time.Minute {
		return nil
	}

	req, err := http.NewRequest(http.MethodGet, oidc.JwksURI, nil)
	if err != nil {
		return err
	}
	res, err := oidc.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code getting JWKS: %d", res.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return err
	}

	var doc jwksDocument
	if err := json.Unmarshal(body, &doc); err != nil {
		return err
	}

	parsedKeys := make(map[string]*rsa.PublicKey, len(doc.Keys))
	for _, key := range doc.Keys {
		if key.Kty != "RSA" || key.N == "" || key.E == "" {
			continue
		}
		pub, err := parseRSAPublicKey(key.N, key.E)
		if err != nil {
			continue
		}
		parsedKeys[key.Kid] = pub
	}
	oidc.keys = parsedKeys
	oidc.lastRefresh = time.Now()
	return nil
}

func parseRSAPublicKey(nStr, eStr string) (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(nStr)
	if err != nil {
		return nil, err
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(eStr)
	if err != nil {
		return nil, err
	}
	eBig := new(big.Int).SetBytes(eBytes)
	if !eBig.IsInt64() || eBig.Int64() <= 1 {
		return nil, errors.New("invalid RSA exponent")
	}
	return &rsa.PublicKey{
		N: new(big.Int).SetBytes(nBytes),
		E: int(eBig.Int64()),
	}, nil
}

func (oidc *RemoteOidcAuthenticator) keyfunc(token *jwt.Token) (any, error) {
	kid, _ := token.Header["kid"].(string)

	oidc.mu.RLock()
	if time.Since(oidc.lastRefresh) < 48*time.Hour {
		if pub, ok := oidc.keys[kid]; ok {
			oidc.mu.RUnlock()
			return pub, nil
		}
		if kid == "" && len(oidc.keys) == 1 {
			for _, pub := range oidc.keys {
				oidc.mu.RUnlock()
				return pub, nil
			}
		}
	}
	oidc.mu.RUnlock()

	_ = oidc.refreshKeys(false)

	oidc.mu.RLock()
	defer oidc.mu.RUnlock()
	if pub, ok := oidc.keys[kid]; ok {
		return pub, nil
	}
	if kid == "" && len(oidc.keys) == 1 {
		for _, pub := range oidc.keys {
			return pub, nil
		}
	}
	return nil, errors.New("matching JWK not found")
}

// Authenticate verifies a Bearer JWT against the configured OIDC provider.
func (oidc *RemoteOidcAuthenticator) Authenticate(requestContext context.Context) (*authclaims.AuthClaims, error) {
	authHeader, err := bearerTokenFromContext(requestContext)
	if err != nil {
		return nil, ErrMissingBearerToken
	}

	jwtParser := jwt.NewParser(
		jwt.WithValidMethods([]string{"RS256"}),
		jwt.WithIssuedAt(),
		jwt.WithExpirationRequired(),
		jwt.WithAudience(oidc.Audience),
	)

	token, err := jwtParser.Parse(authHeader, oidc.keyfunc)
	if err != nil || !token.Valid {
		return nil, errInvalidClaims
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, errInvalidClaims
	}

	validIssuers := append([]string{oidc.MainIssuer}, oidc.IssuerAliases...)
	ok = slices.ContainsFunc(validIssuers, func(issuer string) bool {
		v := jwt.NewValidator(jwt.WithIssuer(issuer))
		return v.Validate(claims) == nil
	})
	if !ok {
		return nil, errInvalidClaims
	}

	if len(oidc.Subjects) > 0 {
		ok = slices.ContainsFunc(oidc.Subjects, func(subject string) bool {
			v := jwt.NewValidator(jwt.WithSubject(subject))
			return v.Validate(claims) == nil
		})
		if !ok {
			return nil, errInvalidClaims
		}
	}

	subject := ""
	if subjectClaim, exists := claims["sub"]; exists {
		subject, ok = subjectClaim.(string)
		if !ok {
			return nil, errInvalidClaims
		}
	}

	clientID := ""
	for _, claimName := range oidc.ClientIDClaims {
		if val, exists := claims[claimName].(string); exists {
			clientID = val
			break
		}
	}

	principal := &authclaims.AuthClaims{
		Subject:  subject,
		Scopes:   make(map[string]bool),
		ClientID: clientID,
	}

	if scopeRaw, exists := claims["scope"]; exists {
		if scopeStr, isStr := scopeRaw.(string); isStr {
			for _, scope := range strings.Split(scopeStr, " ") {
				if scope != "" {
					principal.Scopes[scope] = true
				}
			}
		}
	}

	return principal, nil
}

// Close releases background OIDC resources.
func (*RemoteOidcAuthenticator) Close() {}

// AuthFunc adapts an Authenticator into a context authentication function that
// injects OpenFGA AuthClaims into the request context.
func AuthFunc(authenticator Authenticator) func(context.Context) (context.Context, error) {
	return func(ctx context.Context) (context.Context, error) {
		claims, err := authenticator.Authenticate(ctx)
		if err != nil {
			return nil, err
		}
		return authclaims.ContextWithAuthClaims(ctx, claims), nil
	}
}
