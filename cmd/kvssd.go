package cmd

/*
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <dlfcn.h>

#include "kvs_api.h"

static void minio_kvs_init_env() {
  kvs_init_options options;

  int32_t i;
  i = kvs_init_env_opts(&options);

  options.memory.use_dpdk=0;
  options.aio.iocomplete_fn = NULL;
  options.emul_config_file = "./nvme.conf";

  kvs_init_env(&options);
}

static int32_t minio_kvs_put(kvs_container_handle handle, void *key, int keyLen, void *value, int valueLen) {
    kvs_key kvskey = {key, keyLen};
    kvs_value kvsvalue = {value, valueLen, 0};
    const kvs_store_context put_ctx = { KVS_STORE_POST|KVS_SYNC_IO, 0, NULL, NULL};

    int i = kvs_store_tuple(handle, &kvskey, &kvsvalue, &put_ctx);
    if (i) printf("kvs_store_tuple retval = %d\n", i);
    return i;
}

static int32_t minio_kvs_get(kvs_container_handle handle,  void *key, int keyLen, void *value, int valueLen) {
    kvs_key kvskey = {key, keyLen};
    kvs_value kvsvalue = {value, valueLen, 0};
    const kvs_retrieve_context ret_ctx = { KVS_RETRIEVE_IDEMPOTENT|KVS_SYNC_IO, 0, NULL, NULL };
    int i = kvs_retrieve_tuple(handle, &kvskey, &kvsvalue, &ret_ctx);
    if (i) printf("kvs_retrieve_tuple retval = %d\n", i);
    return i;
}

static int32_t minio_kvs_delete(kvs_container_handle handle,  void *key, int keyLen) {
    kvs_delete_context del_ctx = { KVS_DELETE_TUPLE|KVS_SYNC_IO, 0, NULL, NULL };
    kvs_key kvskey = {key, keyLen};
    int i = kvs_delete_tuple(handle, &kvskey, &del_ctx);
    if (i) printf("kvs_delete_tuple retval = %d\n", i);
    return i;
}

*/
import "C"

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unsafe"
)

func kvs_init_env() {
	C.minio_kvs_init_env()
}

type kvssd struct {
	device            string
	kvDeviceHandle    C.kvs_device_handle
	kvContainerCtx    C.struct___0
	kvContainerHandle C.kvs_container_handle
}

func newKVSSD(device string) (KVAPI, error) {
	if strings.HasPrefix(device, "/dev/xfs") {
		return newKVXFS(device)
	}
	if strings.HasPrefix(device, "/dev/kvemul") {
		device = "/dev/kvemul"
	}
	kvs_init_env()
	k := &kvssd{device: device}
	C.kvs_open_device(C.CString(device), &k.kvDeviceHandle)
	containerStr := C.CString("test")
	C.kvs_create_container(k.kvDeviceHandle, containerStr, 0, &k.kvContainerCtx)
	C.kvs_open_container(k.kvDeviceHandle, containerStr, &k.kvContainerHandle)
	return k, nil
}

func kvKeyName(container, key string) []byte {
	return getSHA256Sum([]byte(pathJoin(container, key)))[:16]
}

func (k *kvssd) Put(container, key string, value []byte) error {
	kvKey := kvKeyName(container, key)
	timer := time.NewTimer(5 * time.Second)
	doneCh := make(chan struct{})
	go func() {
		C.minio_kvs_put(k.kvContainerHandle, unsafe.Pointer(&kvKey[0]), C.int(len(kvKey)), unsafe.Pointer(&value[0]), C.int(len(value)))
		close(doneCh)
	}()
	select {
	case <-timer.C:
		fmt.Printf("put(%s) timedout\n", key)
		return errors.New("timeout")
	case <-doneCh:
		timer.Stop()
		break
	}
	return nil
}

func (k *kvssd) Get(container, key string, value []byte) error {
	kvKey := kvKeyName(container, key)
	timer := time.NewTimer(5 * time.Second)
	doneCh := make(chan struct{})
	go func() {
		C.minio_kvs_get(k.kvContainerHandle, unsafe.Pointer(&kvKey[0]), C.int(len(kvKey)), unsafe.Pointer(&value[0]), C.int(len(value)))
		close(doneCh)
	}()
	select {
	case <-timer.C:
		fmt.Printf("get(%s) timedout\n", key)
		return errors.New("timeout")
	case <-doneCh:
		timer.Stop()
		break
	}
	return nil
}

func (k *kvssd) Delete(container, key string) error {
	kvKey := kvKeyName(container, key)
	timer := time.NewTimer(5 * time.Second)
	doneCh := make(chan struct{})
	go func() {
		C.minio_kvs_delete(k.kvContainerHandle, unsafe.Pointer(&kvKey[0]), C.int(len(kvKey)))
		close(doneCh)
	}()
	select {
	case <-timer.C:
		fmt.Printf("delete(%s) timedout\n", key)
		return errors.New("timeout")
	case <-doneCh:
		timer.Stop()
		break
	}
	return nil
}

func (k *kvssd) List() ([]string, error) {
	return nil, errDiskNotFound
}
