package cmd

import (
	"path"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger"

	xrpc "github.com/minio/minio/cmd/rpc"
)

const kvServiceName = "KV"
const kvServiceSubPath = "/kv"

var kvServicePath = path.Join(minioReservedBucketPath, kvServiceSubPath)

type KVRPCReceiver struct {
	local KVAPI
}

type KVPutArgs struct {
	AuthArgs
	Key   string
	Value []byte
}

func (kv *KVRPCReceiver) Put(args *KVPutArgs, reply *VoidReply) error {
	return kv.local.Put(args.Key, args.Value)
}

type KVGetArgs struct {
	AuthArgs
	Key string
}

type KVGetReply struct {
	Value []byte
}

func (kv *KVRPCReceiver) Get(args *KVGetArgs, reply *KVGetReply) error {
	value, err := kv.local.Get(args.Key)
	reply.Value = value
	return err
}

type KVDeleteArgs = KVGetArgs

func (kv *KVRPCReceiver) Delete(args *KVDeleteArgs, reply *VoidReply) error {
	return kv.local.Delete(args.Key)
}

type KVListReply struct {
	Keys []string
}

func (kv *KVRPCReceiver) List(args *AuthArgs, reply *KVListReply) error {
	return nil
}

func newKVRPCServer(endpointPath string) (*xrpc.Server, error) {
	kv, err := newKVSSD(endpointPath)
	if err != nil {
		return nil, err
	}

	rpcServer := xrpc.NewServer()
	if err = rpcServer.RegisterName(kvServiceName, &KVRPCReceiver{kv}); err != nil {
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
