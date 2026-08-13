// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

func TestHeaderArgumentName(t *testing.T) {
	t.Parallel()

	require.Equal(t, "x_scope_orgid", headerArgumentName("X-Scope-OrgID"))
	require.Equal(t, "x_request_id", headerArgumentName("X-Request-ID"))
	require.Equal(t, "authorization", headerArgumentName("Authorization"))
}

func TestExtractAdvertisedHeaderArgs(t *testing.T) {
	t.Parallel()

	logger, _ := newTestLogger()
	argToHeader := map[string]string{"x_scope_orgid": "X-Scope-OrgID"}

	t.Run("extracts and removes the argument, sets the header", func(t *testing.T) {
		t.Parallel()

		params := &mcp.CallToolParams{Arguments: map[string]any{"query": "up", "x_scope_orgid": "tenant-a"}}
		ctx := extractAdvertisedHeaderArgs(context.Background(), params, argToHeader, logger)

		require.Equal(t, "tenant-a", getForwardedHeadersFromContext(ctx).Get("X-Scope-OrgID"))
		args, ok := params.Arguments.(map[string]any)
		require.True(t, ok)
		require.NotContains(t, args, "x_scope_orgid")
		require.Contains(t, args, "query")
	})

	t.Run("argument overrides a header forwarded from the incoming request", func(t *testing.T) {
		t.Parallel()

		inbound := http.Header{}
		inbound.Set("X-Scope-Orgid", "tenant-from-request")
		ctx := addForwardedHeadersToContext(context.Background(), inbound)

		params := &mcp.CallToolParams{Arguments: map[string]any{"x_scope_orgid": "tenant-from-arg"}}
		ctx = extractAdvertisedHeaderArgs(ctx, params, argToHeader, logger)

		require.Equal(t, "tenant-from-arg", getForwardedHeadersFromContext(ctx).Get("X-Scope-OrgID"))
		// The original inbound header map must not be mutated.
		require.Equal(t, "tenant-from-request", inbound.Get("X-Scope-OrgID"))
	})

	t.Run("handles RawMessage arguments", func(t *testing.T) {
		t.Parallel()

		params := &mcp.CallToolParams{Arguments: json.RawMessage(`{"query":"up","x_scope_orgid":"tenant-b"}`)}
		ctx := extractAdvertisedHeaderArgs(context.Background(), params, argToHeader, logger)

		require.Equal(t, "tenant-b", getForwardedHeadersFromContext(ctx).Get("X-Scope-OrgID"))
		args, ok := params.Arguments.(map[string]any)
		require.True(t, ok)
		require.NotContains(t, args, "x_scope_orgid")
	})

	t.Run("ignores non-string and empty values", func(t *testing.T) {
		t.Parallel()

		params := &mcp.CallToolParams{Arguments: map[string]any{"x_scope_orgid": 42}}
		ctx := extractAdvertisedHeaderArgs(context.Background(), params, argToHeader, logger)
		require.Nil(t, getForwardedHeadersFromContext(ctx))
		// Still removed so schema validation never sees it.
		require.NotContains(t, params.Arguments.(map[string]any), "x_scope_orgid")
	})

	t.Run("no-op without the argument", func(t *testing.T) {
		t.Parallel()

		params := &mcp.CallToolParams{Arguments: map[string]any{"query": "up"}}
		ctx := extractAdvertisedHeaderArgs(context.Background(), params, argToHeader, logger)
		require.Nil(t, getForwardedHeadersFromContext(ctx))
	})
}

func TestAdvertiseHeaderArgs(t *testing.T) {
	t.Parallel()

	argToHeader := map[string]string{"x_scope_orgid": "X-Scope-OrgID"}
	result := &mcp.ListToolsResult{Tools: []*mcp.Tool{
		{Name: "query", InputSchema: &jsonschema.Schema{Type: "object", Properties: map[string]*jsonschema.Schema{
			"query": {Type: "string"},
		}}},
	}}

	registered := result.Tools[0]
	advertiseHeaderArgs(result, argToHeader)
	schema := result.Tools[0].InputSchema.(*jsonschema.Schema)
	require.Contains(t, schema.Properties, "x_scope_orgid")
	require.Equal(t, "string", schema.Properties["x_scope_orgid"].Type)

	// The registered tool's schema must NOT be mutated (the SDK validates
	// against a pre-resolved snapshot of it).
	require.NotContains(t, registered.InputSchema.(*jsonschema.Schema).Properties, "x_scope_orgid")

	// Idempotent on repeat listing.
	advertiseHeaderArgs(result, argToHeader)
	schema = result.Tools[0].InputSchema.(*jsonschema.Schema)
	require.Contains(t, schema.Properties, "x_scope_orgid")
}

// sseResultText extracts the tool result text from a streamable HTTP response body.
func sseResultText(t *testing.T, body string) string {
	t.Helper()
	scanner := bufio.NewScanner(strings.NewReader(body))
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	var last string
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data: ") {
			last = strings.TrimPrefix(line, "data: ")
		} else if strings.HasPrefix(line, "{") {
			last = line // application/json response
		}
	}
	return last
}

// TestAdvertisedHeaders_EndToEnd drives the full flow: streamable HTTP
// handler -> middleware extraction -> schema validation -> handler ->
// per-request API client -> mock Prometheus receiving the header.
func TestAdvertisedHeaders_EndToEnd(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var receivedOrgID string
	promServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		receivedOrgID = r.Header.Get("X-Scope-OrgID")
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
	}))
	defer promServer.Close()

	logger, _ := newTestLogger()
	server, _, err := NewServer(context.Background(), ServerConfig{
		Logger:            logger,
		PrometheusURL:     promServer.URL,
		PrometheusTimeout: 30 * time.Second,
		AdvertisedHeaders: []string{"X-Scope-OrgID"},
	})
	require.NoError(t, err)

	handler := NewStreamableHTTPHandler(server, logger, time.Minute)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	post := func(body, sessionID string) *http.Response {
		req, err := http.NewRequest(http.MethodPost, ts.URL, strings.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		if sessionID != "" {
			req.Header.Set("Mcp-Session-Id", sessionID)
		}
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		return resp
	}

	// Initialize a session.
	initResp := post(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"advtest","version":"0"}}}`, "")
	sid := initResp.Header.Get("Mcp-Session-Id")
	initResp.Body.Close()
	require.NotEmpty(t, sid)
	post(`{"jsonrpc":"2.0","method":"notifications/initialized"}`, sid).Body.Close()

	// tools/list advertises the argument on the query tool.
	listResp := post(`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`, sid)
	listBody := new(strings.Builder)
	_, _ = copyBody(listBody, listResp)
	require.Contains(t, listBody.String(), `"x_scope_orgid"`)

	// tools/call with the argument: validation passes (argument stripped
	// before validation) and the header reaches the Prometheus backend.
	callResp := post(`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"query","arguments":{"query":"up","x_scope_orgid":"tenant-e2e"}}}`, sid)
	callBody := new(strings.Builder)
	_, _ = copyBody(callBody, callResp)
	last := sseResultText(t, callBody.String())
	require.NotContains(t, last, `"isError":true`, "tool call failed: %s", last)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, "tenant-e2e", receivedOrgID)
}

func copyBody(dst *strings.Builder, resp *http.Response) (int64, error) {
	defer resp.Body.Close()
	buf := make([]byte, 32*1024)
	var total int64
	for {
		n, err := resp.Body.Read(buf)
		dst.Write(buf[:n])
		total += int64(n)
		if err != nil {
			return total, nil
		}
	}
}
