// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package semconv

import (
	"net"
	"strconv"

	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

// maxRequestAttrs is the most attributes RedisClientRequestTraceAttrs returns.
const maxRequestAttrs = 6

// RedisRequest describes one Redis client call. Host and Port are the parsed
// server endpoint, see ParseEndpoint. A Port of zero means it is unknown and
// server.port is left out.
type RedisRequest struct {
	Host      string
	Port      int
	FullName  string
	Statement string
}

// ParseEndpoint splits a Redis address such as "localhost:6379" into a host
// and a port. The endpoint does not change once a client exists, so callers
// should parse it once and reuse the result for every command.
//
// If the address has no valid port, the port is 0. If it cannot be split at
// all, the whole address is returned as the host.
func ParseEndpoint(endpoint string) (string, int) {
	host, portStr, err := net.SplitHostPort(endpoint)
	if err != nil {
		return endpoint, 0
	}
	port, convErr := strconv.Atoi(portStr)
	if convErr != nil || port <= 0 {
		return host, 0
	}
	return host, port
}

// RedisClientRequestTraceAttrs returns trace attributes for a Redis client request.
func RedisClientRequestTraceAttrs(req RedisRequest) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, maxRequestAttrs)
	attrs = append(attrs,
		semconv.DBSystemNameRedis,
		semconv.DBOperationName(req.FullName),
		semconv.ServerAddress(req.Host),
		semconv.NetworkTransportTCP,
		semconv.DBQueryText(req.Statement),
	)

	if req.Port > 0 {
		attrs = append(attrs, semconv.ServerPort(req.Port))
	}

	return attrs
}
