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
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// methodToolsList is the MCP method name for listing tools. Declared here
// (not exported by the go-sdk) alongside the advertised-headers middleware
// that post-processes its results.
const methodToolsList = "tools/list"

// advertisedHeadersMiddleware exposes each configured HTTP header as an
// optional per-call tool argument (--mcp.advertise-header):
//
//   - tools/list results gain an optional string property per header (named
//     exactly like the header) on every tool's input schema, so connected
//     LLMs discover the argument.
//   - tools/call requests have the argument extracted (and removed, so tool
//     input validation and handlers never see it) and stored in the request
//     context as a forwarded header, where per-request API clients pick it up
//     exactly like a header forwarded from the incoming HTTP request — but
//     with higher precedence.
//
// This lets a single server/session target e.g. different tenants of a
// multi-tenant Prometheus-compatible backend per tool call by setting
// X-Scope-OrgID, without reconnecting or reconfiguring the client.
func advertisedHeadersMiddleware(headers []string, logger *slog.Logger) mcp.Middleware {
	// The advertised argument name is the header name, verbatim — no
	// derivation rule for operators or models to learn. (JSON property names
	// may contain '-' and capitals; the schema tells the model the exact
	// name either way.)
	argToHeader := make(map[string]string, len(headers))
	for _, h := range headers {
		argToHeader[h] = h
	}

	return func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			switch method {
			case methodToolsCall:
				// Server-side dispatch delivers *CallToolParamsRaw (arguments
				// still raw JSON); *CallToolParams is handled too for
				// in-process callers.
				switch params := req.GetParams().(type) {
				case *mcp.CallToolParamsRaw:
					ctx = extractAdvertisedHeaderArgsRaw(ctx, params, argToHeader, logger)
				case *mcp.CallToolParams:
					ctx = extractAdvertisedHeaderArgs(ctx, params, argToHeader, logger)
				}
				return next(ctx, method, req)
			case methodToolsList:
				result, err := next(ctx, method, req)
				if err == nil {
					if listResult, ok := result.(*mcp.ListToolsResult); ok && listResult != nil {
						advertiseHeaderArgs(listResult, argToHeader)
					}
				}
				return result, err
			}
			return next(ctx, method, req)
		}
	}
}

// pullAdvertisedArgs removes advertised header arguments from args and merges
// them into the context's forwarded headers, overriding any same-named header
// captured from the incoming HTTP request. It reports whether args changed.
func pullAdvertisedArgs(ctx context.Context, args map[string]any, argToHeader map[string]string, logger *slog.Logger) (context.Context, bool) {
	var merged http.Header
	changed := false
	for argName, header := range argToHeader {
		raw, present := args[argName]
		if !present {
			continue
		}
		delete(args, argName)
		changed = true

		value, ok := raw.(string)
		if !ok || value == "" {
			logger.Warn("Ignoring advertised header argument with non-string or empty value", "argument", argName, "header", header)
			continue
		}

		if merged == nil {
			merged = getForwardedHeadersFromContext(ctx).Clone()
			if merged == nil {
				merged = make(http.Header)
			}
		}
		merged.Set(header, value)
	}

	if merged != nil {
		ctx = addForwardedHeadersToContext(ctx, merged)
	}
	return ctx, changed
}

// extractAdvertisedHeaderArgsRaw handles the server-side dispatch shape,
// where tool arguments are still raw JSON: advertised arguments are removed
// before the SDK validates the raw arguments against the tool's input schema.
func extractAdvertisedHeaderArgsRaw(ctx context.Context, params *mcp.CallToolParamsRaw, argToHeader map[string]string, logger *slog.Logger) context.Context {
	if len(params.Arguments) == 0 {
		return ctx
	}
	var args map[string]any
	if err := json.Unmarshal(params.Arguments, &args); err != nil {
		return ctx
	}

	ctx, changed := pullAdvertisedArgs(ctx, args, argToHeader, logger)
	if changed {
		if data, err := json.Marshal(args); err == nil {
			params.Arguments = data
		}
	}
	return ctx
}

// extractAdvertisedHeaderArgs handles the client-side params shape (used by
// in-process callers and tests), where arguments may already be decoded.
func extractAdvertisedHeaderArgs(ctx context.Context, params *mcp.CallToolParams, argToHeader map[string]string, logger *slog.Logger) context.Context {
	var args map[string]any
	switch v := params.Arguments.(type) {
	case map[string]any:
		args = v
	case json.RawMessage:
		if err := json.Unmarshal(v, &args); err != nil {
			return ctx
		}
	case []byte:
		if err := json.Unmarshal(v, &args); err != nil {
			return ctx
		}
	default:
		return ctx
	}

	ctx, _ = pullAdvertisedArgs(ctx, args, argToHeader, logger)

	// Write the cleaned arguments back (required for the RawMessage/[]byte
	// cases; harmless for the in-place map case).
	params.Arguments = args
	return ctx
}

// advertiseHeaderArgs appends the advertised header arguments as optional
// string properties to every tool's input schema in a tools/list result.
//
// The result entries are COPIES: the registered tools' schemas must never be
// mutated — the SDK validates calls against a pre-resolved snapshot of the
// registration-time schema, and mutating the raw schema afterwards
// desynchronizes the two (nil dereference in ApplyDefaults). The argument is
// stripped from calls before validation, so the advertised property never
// needs to exist in the validation schema at all.
func advertiseHeaderArgs(result *mcp.ListToolsResult, argToHeader map[string]string) {
	for i, tool := range result.Tools {
		schema, ok := tool.InputSchema.(*jsonschema.Schema)
		if !ok || schema == nil {
			continue
		}

		schemaCopy := *schema
		properties := make(map[string]*jsonschema.Schema, len(schema.Properties)+len(argToHeader))
		for name, prop := range schema.Properties {
			properties[name] = prop
		}
		for argName, header := range argToHeader {
			if _, exists := properties[argName]; exists {
				continue
			}
			properties[argName] = &jsonschema.Schema{
				Type: "string",
				Description: fmt.Sprintf("Optional. Sets the %s HTTP header on the backend Prometheus API request for this call only,"+
					" overriding any value forwarded from the incoming request"+
					" (server flag --mcp.advertise-header=%s).", header, header),
			}
		}
		schemaCopy.Properties = properties

		toolCopy := *tool
		toolCopy.InputSchema = &schemaCopy
		result.Tools[i] = &toolCopy
	}
}
