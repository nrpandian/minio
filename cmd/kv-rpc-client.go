package cmd

import (
	"context"
	"crypto/tls"
	"path"

	"github.com/minio/minio/cmd/logger"

	xnet "github.com/minio/minio/pkg/net"
)

type kvRPCClient struct {
	*RPCClient
}

func (kv *kvRPCClient) Put(key string, value []byte) error {
	args := KVPutArgs{Key: key, Value: value}
	err := kv.Call(kvServiceName+".Put", &args, &VoidReply{})
	return err
}

func (kv *kvRPCClient) Get(key string) ([]byte, error) {
	args := KVGetArgs{Key: key}
	reply := KVGetReply{}
	err := kv.Call(kvServiceName+".Get", &args, &reply)
	return reply.Value, err
}

func (kv *kvRPCClient) Delete(key string) error {
	args := KVDeleteArgs{Key: key}
	err := kv.Call(kvServiceName+".Delete", &args, &VoidReply{})
	return err
}

func (kv *kvRPCClient) List() ([]string, error) {
	return nil, errFileNotFound
}

func newKVRPCClient(host *xnet.Host, endpointPath string) (*kvRPCClient, error) {
	scheme := "http"
	if globalIsSSL {
		scheme = "https"
	}

	serviceURL := &xnet.URL{
		Scheme: scheme,
		Host:   host.String(),
		Path:   path.Join(kvServicePath, endpointPath),
	}

	var tlsConfig *tls.Config
	if globalIsSSL {
		tlsConfig = &tls.Config{
			ServerName: host.Name,
			RootCAs:    globalRootCAs,
		}
	}

	rpcClient, err := NewRPCClient(
		RPCClientArgs{
			NewAuthTokenFunc: newAuthToken,
			RPCVersion:       globalRPCAPIVersion,
			ServiceName:      kvServiceName,
			ServiceURL:       serviceURL,
			TLSConfig:        tlsConfig,
		},
	)
	if err != nil {
		return nil, err
	}

	return &kvRPCClient{RPCClient: rpcClient}, nil

}

func newKVRPC(endpoint Endpoint) *kvRPCClient {
	host, err := xnet.ParseHost(endpoint.Host)
	logger.FatalIf(err, "Unable to parse KV RPC Host", context.Background())
	rpcClient, err := newKVRPCClient(host, endpoint.Path)
	logger.FatalIf(err, "Unable to initialize storage RPC client", context.Background())
	return rpcClient
}
