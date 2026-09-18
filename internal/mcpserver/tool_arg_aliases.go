package mcpserver

import (
	"context"
	"encoding/json"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// toolArgAliases maps silently-accepted argument aliases to their canonical
// parameter names, per tool. Aliases are not advertised in input schemas; they
// let callers that guess common alternative names (such as to_address for
// waypost_send's to) succeed instead of failing schema validation.
var toolArgAliases = map[string]map[string]string{
	"waypost_send": {
		"to_address": "to",
	},
}

// normalizeToolArgAliases rewrites aliased tool arguments in incoming calls
// before SDK schema validation runs, so an alias validates as its canonical
// property. When the canonical name is already present it wins and the alias
// is left in place for validation to report.
func normalizeToolArgAliases(next mcp.MethodHandler) mcp.MethodHandler {
	return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
		if method == "tools/call" {
			if callReq, ok := req.(*mcp.CallToolRequest); ok && callReq.Params != nil {
				if aliases, ok := toolArgAliases[callReq.Params.Name]; ok {
					callReq.Params.Arguments = rewriteArgAliases(callReq.Params.Arguments, aliases)
				}
			}
		}
		return next(ctx, method, req)
	}
}

func rewriteArgAliases(raw json.RawMessage, aliases map[string]string) json.RawMessage {
	if len(raw) == 0 {
		return raw
	}
	var args map[string]json.RawMessage
	if err := json.Unmarshal(raw, &args); err != nil {
		return raw
	}
	changed := false
	for alias, canonical := range aliases {
		if _, ok := args[canonical]; ok {
			continue
		}
		value, ok := args[alias]
		if !ok {
			continue
		}
		args[canonical] = value
		delete(args, alias)
		changed = true
	}
	if !changed {
		return raw
	}
	rewritten, err := json.Marshal(args)
	if err != nil {
		return raw
	}
	return rewritten
}
