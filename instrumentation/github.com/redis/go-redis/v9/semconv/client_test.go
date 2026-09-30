// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package semconv

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisClientRequestTraceAttrs(t *testing.T) {
	tests := []struct {
		name     string
		req      RedisRequest
		expected map[string]interface{}
	}{
		{
			name: "basic GET command",
			req: RedisRequest{
				Host:      "localhost",
				Port:      6379,
				FullName:  "get",
				Statement: "get mykey",
			},
			expected: map[string]interface{}{
				"db.system.name":    "redis",
				"db.operation.name": "get",
				"server.address":    "localhost",
				"server.port":       int64(6379),
				"network.transport": "tcp",
				"db.query.text":     "get mykey",
			},
		},
		{
			name: "SET command with value",
			req: RedisRequest{
				Host:      "redis.example.com",
				Port:      6380,
				FullName:  "set",
				Statement: "set mykey myvalue",
			},
			expected: map[string]interface{}{
				"db.system.name":    "redis",
				"db.operation.name": "set",
				"server.address":    "redis.example.com",
				"server.port":       int64(6380),
				"network.transport": "tcp",
				"db.query.text":     "set mykey myvalue",
			},
		},
		{
			name: "HSET command",
			req: RedisRequest{
				Host:      "127.0.0.1",
				Port:      6379,
				FullName:  "hset",
				Statement: "hset myhash field1 value1",
			},
			expected: map[string]interface{}{
				"db.system.name":    "redis",
				"db.operation.name": "hset",
				"server.address":    "127.0.0.1",
				"server.port":       int64(6379),
				"network.transport": "tcp",
				"db.query.text":     "hset myhash field1 value1",
			},
		},
		{
			name: "pipeline command",
			req: RedisRequest{
				Host:      "localhost",
				Port:      6379,
				FullName:  "pipeline",
				Statement: "pipeline get/set/del/...",
			},
			expected: map[string]interface{}{
				"db.system.name":    "redis",
				"db.operation.name": "pipeline",
				"server.address":    "localhost",
				"server.port":       int64(6379),
				"network.transport": "tcp",
				"db.query.text":     "pipeline get/set/del/...",
			},
		},
		{
			name: "empty fields",
			req: RedisRequest{
				Host:      "",
				FullName:  "",
				Statement: "",
			},
			expected: map[string]interface{}{
				"db.system.name":    "redis",
				"db.operation.name": "",
				"server.address":    "",
				"network.transport": "tcp",
				"db.query.text":     "",
			},
		},
		{
			name: "endpoint without port",
			req: RedisRequest{
				Host:      "redis.local",
				FullName:  "ping",
				Statement: "ping",
			},
			expected: map[string]interface{}{
				"db.system.name":    "redis",
				"db.operation.name": "ping",
				"server.address":    "redis.local",
				"network.transport": "tcp",
				"db.query.text":     "ping",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attrs := RedisClientRequestTraceAttrs(tt.req)

			attrMap := make(map[string]interface{})
			for _, attr := range attrs {
				attrMap[string(attr.Key)] = attr.Value.AsInterface()
			}

			require.Len(t, attrMap, len(tt.expected), "attribute count mismatch")

			for key, expectedVal := range tt.expected {
				actualVal, ok := attrMap[key]
				require.True(t, ok, "expected attribute %s not found", key)
				assert.Equal(t, expectedVal, actualVal, "attribute %s value mismatch", key)
			}
		})
	}
}

func TestRedisClientRequestTraceAttrs_ContainsDBSystemRedis(t *testing.T) {
	req := RedisRequest{
		Host:      "localhost",
		Port:      6379,
		FullName:  "get",
		Statement: "get key",
	}

	attrs := RedisClientRequestTraceAttrs(req)

	found := false
	for _, attr := range attrs {
		if string(attr.Key) == "db.system.name" && attr.Value.AsString() == "redis" {
			found = true
			break
		}
	}
	assert.True(t, found, "should contain db.system.name=redis attribute")
}

func TestRedisClientRequestTraceAttrs_ContainsNetworkTransportTCP(t *testing.T) {
	req := RedisRequest{
		Host:      "localhost",
		Port:      6379,
		FullName:  "get",
		Statement: "get key",
	}

	attrs := RedisClientRequestTraceAttrs(req)

	found := false
	for _, attr := range attrs {
		if string(attr.Key) == "network.transport" && attr.Value.AsString() == "tcp" {
			found = true
			break
		}
	}
	assert.True(t, found, "should contain network.transport=tcp attribute")
}

func TestParseEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		host     string
		port     int
	}{
		{name: "host and port", endpoint: "localhost:6379", host: "localhost", port: 6379},
		{name: "ipv4", endpoint: "127.0.0.1:6380", host: "127.0.0.1", port: 6380},
		{name: "ipv6", endpoint: "[::1]:6379", host: "::1", port: 6379},
		{name: "no port", endpoint: "redis.local", host: "redis.local", port: 0},
		{name: "empty", endpoint: "", host: "", port: 0},
		{name: "non numeric port", endpoint: "redis.local:abc", host: "redis.local", port: 0},
		{name: "zero port", endpoint: "redis.local:0", host: "redis.local", port: 0},
		{name: "empty port", endpoint: "redis.local:", host: "redis.local", port: 0},
		{name: "unix socket", endpoint: "/var/run/redis.sock", host: "/var/run/redis.sock", port: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, port := ParseEndpoint(tt.endpoint)
			assert.Equal(t, tt.host, host)
			assert.Equal(t, tt.port, port)
		})
	}
}

func TestRedisClientRequestTraceAttrs_NoPortOmitsServerPort(t *testing.T) {
	attrs := RedisClientRequestTraceAttrs(RedisRequest{Host: "redis.local", FullName: "get"})

	for _, attr := range attrs {
		assert.NotEqual(t, "server.port", string(attr.Key))
	}
}

func BenchmarkRedisClientRequestTraceAttrs(b *testing.B) {
	host, port := ParseEndpoint("localhost:6379")
	req := RedisRequest{Host: host, Port: port, FullName: "get", Statement: "get mykey"}

	b.ReportAllocs()
	for b.Loop() {
		RedisClientRequestTraceAttrs(req)
	}
}
