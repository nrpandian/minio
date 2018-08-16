package cmd

/*
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <dlfcn.h>

#include "kvs_types.h"

static void minio_kvs_init_env() {
  kvs_init_options options;

  int32_t i;
  i = kvs_init_env_opts(&options);

  options.memory.use_dpdk=0;

  options.aio.iocoremask=1;
  options.aio.queuedepth=64;
  memset(options.udd.core_mask_str, 0, 256);
  options.udd.core_mask_str[0] = 123;
  options.udd.core_mask_str[1] = 125;
  memset(options.udd.cq_thread_mask, 0, 256);
  options.udd.cq_thread_mask[0] = 50;

  kvs_init_env(&options);
}

static struct kv_device_api* minio_kvs_open_device(char *device) {
    kvs_open_device(device, "./nvme.conf");
}

static void minio_kvs_close_device(struct kv_device_api* kvd) {
    kvs_close_device(kvd);
}

static int32_t minio_kvs_put(struct kv_device_api *kvd, void *key, int keyLen, void *value, int valueLen) {
    kvs_key kvskey = {key, keyLen};
    kvs_value kvsvalue = {value, valueLen, 0};
    const kvs_store_context put_ctx = { KVS_STORE_POST|KVS_SYNC_IO, 0, NULL, NULL};

    int i = kvs_store_tuple(kvd, &kvskey, &kvsvalue, &put_ctx);
    if (i) printf("kvs_store_tuple retval = %d\n", i);
    return i;
}

static int32_t minio_kvs_get(struct kv_device_api *kvd,  void *key, int keyLen, void *value, int valueLen) {
    kvs_key kvskey = {key, keyLen};
    kvs_value kvsvalue = {value, valueLen, 0};
    const kvs_retrieve_context ret_ctx = { KVS_RETRIEVE_IDEMPOTENT|KVS_SYNC_IO, 0, NULL, NULL };
    int i = kvs_retrieve_tuple(kvd, &kvskey, &kvsvalue, &ret_ctx);
    if (i) printf("kvs_retrieve_tuple retval = %d\n", i);
    return i;
}

static int32_t minio_kvs_delete(struct kv_device_api *kvd,  void *key, int keyLen) {
    kvs_delete_context del_ctx = { KVS_DELETE_TUPLE|KVS_SYNC_IO, 0, NULL, NULL };
    kvs_key kvskey = {key, keyLen};
    int i = kvs_delete_tuple(kvd, &kvskey, &del_ctx);
    if (i) printf("kvs_delete_tuple retval = %d\n", i);
    return i;
}

*/
import "C"

import (
	"strings"
	"unsafe"
)

func kvs_init_env() {
	C.minio_kvs_init_env()
}

func kvs_open_device(device string) *C.struct_kv_device_api {
	deviceCstr := C.CString(device)
	return C.minio_kvs_open_device(deviceCstr)
}

func kvs_close_device(kvd *C.struct_kv_device_api) {
	C.minio_kvs_close_device(kvd)
}

type kvssd struct {
	device string
	kvd    *C.struct_kv_device_api
}

func newKVSSD(device string) (KVAPI, error) {
	if strings.HasPrefix(device, "/dev/xfs") {
		return newKVXFS(device)
	}
	if strings.HasPrefix(device, "/dev/kvemul") {
		device = "/dev/kvemul"
	}
	kvd := kvs_open_device(device)
	if kvd == nil {
		return nil, errDiskNotFound
	}
	return &kvssd{device, kvd}, nil
}

func kvKeyName(container, key string) []byte {
	return getSHA256Sum([]byte(pathJoin(container, key)))[:16]
}

func (k *kvssd) Put(container, key string, value []byte) error {
	kvKey := kvKeyName(container, key)
	C.minio_kvs_put(k.kvd, unsafe.Pointer(&kvKey[0]), C.int(len(kvKey)), unsafe.Pointer(&value[0]), C.int(len(value)))
	return nil
}

func (k *kvssd) Get(container, key string, value []byte) error {
	kvKey := kvKeyName(container, key)
	C.minio_kvs_get(k.kvd, unsafe.Pointer(&kvKey[0]), C.int(len(kvKey)), unsafe.Pointer(&value[0]), C.int(len(value)))
	return nil
}

func (k *kvssd) Delete(container, key string) error {
	kvKey := kvKeyName(container, key)
	C.minio_kvs_delete(k.kvd, unsafe.Pointer(&kvKey[0]), C.int(len(kvKey)))
	return nil
}

func (k *kvssd) List() ([]string, error) {
	return nil, errDiskNotFound
}
