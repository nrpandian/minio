package cmd

import (
	"path"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger"

	xrpc "github.com/minio/minio/cmd/rpc"
)

const kvServiceName = "KV"
const kvServiceSubPath = "/kv"

var kvServicePath = path.Join(minioReservedBucketPath, storageServiceSubPath)

type kvRPCReceiver struct {
	local KVAPI
}

type kvPutArgs struct {
	AuthArgs
	Key   string
	Value []byte
}

func (kv *kvRPCReceiver) Put(args *kvPutArgs, reply *VoidReply) error {
	return nil
}

type kvGetArgs struct {
	AuthArgs
	Key string
}

type kvGetReply struct {
	Value []byte
}

func (kv *kvRPCReceiver) Get(args *kvGetArgs, reply *kvGetReply) error {
	return nil
}

type kvDeleteArgs = kvGetArgs

func (kv *kvRPCReceiver) Delete(args *kvDeleteArgs, reply *VoidReply) error {
	return nil
}

type kvListReply struct {
	Keys []string
}

func (kv *kvRPCReceiver) List(args *AuthArgs, reply *kvListReply) error {
	return nil
}

func newKVRPCServer(endpointPath string) (*xrpc.Server, error) {
	kv, err := newKVSSD(endpointPath)
	if err != nil {
		return nil, err
	}

	rpcServer := xrpc.NewServer()
	if err = rpcServer.RegisterName(kvServiceName, &kvRPCReceiver{kv}); err != nil {
		return nil, err
	}

	return rpcServer, nil

}

func registerKVRPCRouters(router *mux.Router, endpoints EndpointList) {
	for _, endpoint := range endpoints {
		if !endpoint.IsLocal {
			continue
		}
		rpcServer, err := newKVRPCServer(endpoint.Path)
		if err != nil {
			logger.Fatal(uiErrUnableToWriteInBackend(err), "Unable to configure one of server's RPC services")
		}
		subrouter := router.PathPrefix(minioReservedBucketPath).Subrouter()
		subrouter.Path(path.Join(kvServiceSubPath, endpoint.Path)).Handler(rpcServer)
	}
}
