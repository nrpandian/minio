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

extern void on_io_complete_callback();

static kvs_iterator_handle *handle;
static kvs_iterator_list iter_list;

static void on_io_complete(kv_iocb* ioctx) {
    const char *errStr = NULL;
    if(ioctx->result != 0) {
        errStr = kvs_errstr(ioctx->result);
    } else if (ioctx->opcode == IOCB_ASYNC_ITER_OPEN_CMD) {
        handle = ioctx->iter_handle;
        return;
    }
    on_io_complete_callback(ioctx->private1, errStr);
}

static void minio_kvs_init_env() {
  printf("init called\n");
  kvs_init_options options;

  int32_t i;
  i = kvs_init_env_opts(&options);

  options.memory.use_dpdk=0;

  options.aio.iocoremask=1;
  options.aio.queuedepth=64;
  // options.aio.iocomplete_fn=on_io_complete;
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

static int32_t minio_kvs_put(struct kv_device_api *kvd, void *key, int keyLen, void *value, int valueLen, uint64_t chPtr) {
    kvs_key kvskey = {key, keyLen};
    kvs_value kvsvalue = {value, valueLen, 0};
    const kvs_store_context put_ctx = { KVS_STORE_POST|KVS_SYNC_IO, 0, (void*)chPtr, NULL};

    int i = kvs_store_tuple(kvd, &kvskey, &kvsvalue, &put_ctx);
    if (i) printf("kvs_store_tuple retval = %d\n", i);
}

static int32_t minio_kvs_get(struct kv_device_api *kvd,  void *key, int keyLen, void *value, int valueLen, uint64_t chPtr) {
    kvs_key kvskey = {key, keyLen};
    kvs_value kvsvalue = {value, valueLen, 0};
    const kvs_retrieve_context ret_ctx = { KVS_RETRIEVE_IDEMPOTENT|KVS_SYNC_IO, 0, (void*)chPtr, NULL };
    int i = kvs_retrieve_tuple(kvd, &kvskey, &kvsvalue, &ret_ctx);
    if (i) printf("kvs_retrieve_tuple retval = %d\n", i);
}

static int32_t minio_kvs_delete(struct kv_device_api *kvd,  void *key, int keyLen, uint64_t chPtr) {
    kvs_delete_context del_ctx = { KVS_DELETE_TUPLE|KVS_SYNC_IO, 0, NULL, NULL };
    kvs_key kvskey = {key, keyLen};
    int i = kvs_delete_tuple(kvd, &kvskey, &del_ctx);
    if (i) printf("kvs_delete_tuple retval = %d\n", i);
}

static int32_t minio_kvs_list(struct kv_device_api *kvd,  void *value, uint64_t chPtr) {
    kvs_iterator_context iter_ctx;
    iter_ctx.bitmask = 0x00000000;
    iter_ctx.bit_pattern = 0x00000000;
    iter_ctx.option = KVS_ITERATOR_OPT_KEY;
    int32_t i = kvs_open_iterator(kvd, &iter_ctx);
    if (i) printf("kvs_open_iterator retval = %d\n", i);
    while (kvs_get_ioevents(kvd, 1) == 0){};
    iter_list.it_list = (uint8_t*) value;
    iter_list.size = 32*1024;
    iter_ctx.option = KVS_ITER_DEFAULT;
    iter_ctx.private1 = (void*)chPtr;
    kvs_iterator_next(kvd, handle, &iter_list, &iter_ctx);
    while (kvs_get_ioevents(kvd, 1) == 0){};
    kvs_close_iterator(kvd, handle, &iter_ctx);
    handle = NULL;
}

static int32_t minio_kvs_get_ioevents(struct kv_device_api *kvd, int numEvents) {
    return kvs_get_ioevents(kvd, numEvents);
}

*/
import "C"

import (
	"errors"
	"strings"
	"time"
	"unsafe"
)

type chanContainer struct {
	c chan error
}

//export on_io_complete_callback
func on_io_complete_callback(chPtr unsafe.Pointer, errStr *C.char) {
	var err error
	if errStr != nil {
		err = errors.New(C.GoString(errStr))
	}
	if chPtr == nil {
		return
	}
	chContainer := (*chanContainer)(chPtr)
	go func() {
		chContainer.c <- err
		// close(chContainer.c)
		// FiXME: take care of kvs_open_iterator cbk
	}()
}

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

func kvs_put(kvd *C.struct_kv_device_api, key string, value []byte) error {
	key = key + ":"
	keyCStrinc := C.CString(key)
	errCh := make(chan error)
	chContainer := chanContainer{errCh}
	C.minio_kvs_put(kvd, unsafe.Pointer(keyCStrinc), C.int(len(key)), unsafe.Pointer(&value[0]), C.int(len(value)), C.ulong(uintptr(unsafe.Pointer(&chContainer))))
	return nil
}

func kvs_get(kvd *C.struct_kv_device_api, key string) ([]byte, error) {
	key = key + ":"
	keyBytes := []byte(key)
	valueLen := 2 * 1024 * 1024
	value := make([]byte, valueLen)
	errCh := make(chan error, 1)
	chContainer := chanContainer{errCh}
	C.minio_kvs_get(kvd, unsafe.Pointer(&keyBytes[0]), C.int(len(keyBytes)), unsafe.Pointer(&value[0]), C.int(len(value)), C.ulong(uintptr(unsafe.Pointer(&chContainer))))
	return value, nil
}

func kvs_delete(kvd *C.struct_kv_device_api, key string) error {
	key = key + ":"
	keyBytes := []byte(key)
	errCh := make(chan error, 1)
	chContainer := chanContainer{errCh}
	C.minio_kvs_delete(kvd, unsafe.Pointer(&keyBytes[0]), C.int(len(keyBytes)), C.ulong(uintptr(unsafe.Pointer(&chContainer))))
	return nil
}

func kvs_list(kvd *C.struct_kv_device_api) ([]byte, error) {
	valueLen := 2 * 1024 * 1024
	value := make([]byte, valueLen)

	errCh := make(chan error, 1)
	chContainer := chanContainer{errCh}
	C.minio_kvs_list(kvd, unsafe.Pointer(&value[0]), C.ulong(uintptr(unsafe.Pointer(&chContainer))))
	for C.minio_kvs_get_ioevents(kvd, 1) == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	err := <-errCh
	return value, err
}

type kvssd struct {
	device string
	kvd    *C.struct_kv_device_api
}

func newKVSSD(device string) (*kvssd, error) {
	if strings.HasPrefix(device, "/dev/kvemul") {
		device = "/dev/kvemul"
	}
	kvd := kvs_open_device(device)
	if kvd == nil {
		return nil, errDiskNotFound
	}
	return &kvssd{device, kvd}, nil
}

func (k *kvssd) Put(key string, value []byte) error {
	return kvs_put(k.kvd, key, value)
}

func (k *kvssd) Get(key string) ([]byte, error) {
	return kvs_get(k.kvd, key)
}

func (k *kvssd) Delete(key string) error {
	return kvs_delete(k.kvd, key)
}

func (k *kvssd) List() ([]string, error) {
	return nil, errFileNotFound
}
