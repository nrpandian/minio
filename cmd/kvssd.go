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

static void on_io_complete(kv_iocb* ioctx) {
    const char *errStr = NULL;
    if(ioctx->result != 0) {
        errStr = kvs_errstr(ioctx->result);
    }
    on_io_complete_callback(ioctx->private1, errStr);
}

static void minio_kvs_init_env() {
  kvs_init_options options;

  int32_t i;
  i = kvs_init_env_opts(&options);

  options.memory.use_dpdk=0;

  options.aio.iocoremask=1;
  options.aio.queuedepth=64;
  options.aio.iocomplete_fn=on_io_complete;
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
    const kvs_store_context put_ctx = { KVS_STORE_POST, 0, (void*)chPtr, NULL};

    int i = kvs_store_tuple(kvd, &kvskey, &kvsvalue, &put_ctx);
    if (i) printf("kvs_store_tuple retval = %d\n", i);
    return i;
}

static int32_t minio_kvs_get(struct kv_device_api *kvd,  void *key, int keyLen, void *value, int valueLen, uint64_t chPtr) {
    kvs_key kvskey = {key, keyLen};
    kvs_value kvsvalue = {value, valueLen, 0};
    const kvs_retrieve_context ret_ctx = { KVS_RETRIEVE_IDEMPOTENT, 0, (void*)chPtr, NULL };
    int i = kvs_retrieve_tuple(kvd, &kvskey, &kvsvalue, &ret_ctx);
    if (i) printf("kvs_retrieve_tuple retval = %d\n", i);
    return i;
}

static int32_t minio_kvs_delete(struct kv_device_api *kvd,  void *key, int keyLen, uint64_t chPtr) {
    kvs_delete_context del_ctx = { KVS_DELETE_TUPLE, 0, (void*)chPtr, NULL };
    kvs_key kvskey = {key, keyLen};
    int i = kvs_delete_tuple(kvd, &kvskey, &del_ctx);
    if (i) printf("kvs_delete_tuple retval = %d\n", i);
    return i;
}

static int32_t minio_kvs_get_ioevents(struct kv_device_api *kvd, int numEvents) {
    return kvs_get_ioevents(kvd, numEvents);
}

*/
import "C"

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"unsafe"
)

//export on_io_complete_callback
func on_io_complete_callback(chPtr unsafe.Pointer, errStr *C.char) {
	var err error
	if errStr != nil {
		err = errors.New(C.GoString(errStr))
	}
	if chPtr == nil {
		fmt.Println("chPtr is nil")
		return
	}
	chContainer := (*chanContainer)(chPtr)
	chContainer.KVIOCH <- KVIO{callType: KVCallback, chContainer: chContainer, err: err}
}

type KVCallType int

const (
	KVPut KVCallType = iota
	KVGet
	KVDelete
	KVCallback
)

type chanContainer struct {
	c      chan error
	KVIOCH chan KVIO
}

type KVIO struct {
	callType    KVCallType
	key         []byte
	value       []byte
	chContainer *chanContainer
	err         error
}

var kvssdMap = make(map[string]*kvssd)

func (k *kvssd) kv_io() {
	runtime.LockOSThread()
	callCount := 0
	chanContainerMap := make(map[*chanContainer]bool)
	for {
		if callCount == 0 {
			select {
			case kvio := <-k.KVIOCH:
				switch kvio.callType {
				case KVPut:
					callCount++
					chanContainerMap[kvio.chContainer] = true
					C.minio_kvs_put(k.kvd, unsafe.Pointer(&kvio.key[0]), C.int(len(kvio.key)), unsafe.Pointer(&kvio.value[0]), C.int(len(kvio.value)), C.ulong(uintptr(unsafe.Pointer(kvio.chContainer))))
				case KVGet:
					callCount++
					chanContainerMap[kvio.chContainer] = true
					C.minio_kvs_get(k.kvd, unsafe.Pointer(&kvio.key[0]), C.int(len(kvio.key)), unsafe.Pointer(&kvio.value[0]), C.int(len(kvio.value)), C.ulong(uintptr(unsafe.Pointer(kvio.chContainer))))
				case KVDelete:
					callCount++
					chanContainerMap[kvio.chContainer] = true
					C.minio_kvs_delete(k.kvd, unsafe.Pointer(&kvio.key[0]), C.int(len(kvio.key)), C.ulong(uintptr(unsafe.Pointer(kvio.chContainer))))
				}
			}
		} else {
			select {
			case kvio := <-k.KVIOCH:
				switch kvio.callType {
				case KVPut:
					callCount++
					chanContainerMap[kvio.chContainer] = true
					C.minio_kvs_put(k.kvd, unsafe.Pointer(&kvio.key[0]), C.int(len(kvio.key)), unsafe.Pointer(&kvio.value[0]), C.int(len(kvio.value)), C.ulong(uintptr(unsafe.Pointer(kvio.chContainer))))
				case KVGet:
					callCount++
					chanContainerMap[kvio.chContainer] = true
					C.minio_kvs_get(k.kvd, unsafe.Pointer(&kvio.key[0]), C.int(len(kvio.key)), unsafe.Pointer(&kvio.value[0]), C.int(len(kvio.value)), C.ulong(uintptr(unsafe.Pointer(kvio.chContainer))))
				case KVDelete:
					callCount++
					chanContainerMap[kvio.chContainer] = true
					C.minio_kvs_delete(k.kvd, unsafe.Pointer(&kvio.key[0]), C.int(len(kvio.key)), C.ulong(uintptr(unsafe.Pointer(kvio.chContainer))))
				case KVCallback:
					callCount--
					kvio.chContainer.c <- kvio.err
					delete(chanContainerMap, kvio.chContainer)
				}
			default:
				C.minio_kvs_get_ioevents(k.kvd, 1)
			}
		}
	}
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

type kvssd struct {
	device string
	kvd    *C.struct_kv_device_api
	KVIOCH chan KVIO
}

func newKVSSD(device string) (KVAPI, error) {
	if strings.HasPrefix(device, "/dev/xfs") {
		return newKVXFS(device)
	}
	if strings.HasPrefix(device, "/dev/kvemul") {
		device = "/dev/kvemul"
	}
	k := kvssdMap[device]
	if k != nil {
		return k, nil
	}
	KVIOCH := make(chan KVIO, 10)
	kvd := kvs_open_device(device)
	if kvd == nil {
		return nil, errDiskNotFound
	}
	k = &kvssd{device, kvd, KVIOCH}
	go k.kv_io()
	kvssdMap[device] = k
	return k, nil
}

func kvKeyName(container, key string) []byte {
	return getSHA256Sum([]byte(pathJoin(container, key)))[:16]
}

func (k *kvssd) Put(container, key string, value []byte) error {
	kvKey := kvKeyName(container, key)
	c := make(chan error)
	chContainer := &chanContainer{c, k.KVIOCH}
	k.KVIOCH <- KVIO{callType: KVPut, key: kvKey, value: value, chContainer: chContainer}
	return <-chContainer.c
}

func (k *kvssd) Get(container, key string, value []byte) error {
	kvKey := kvKeyName(container, key)
	c := make(chan error)
	chContainer := &chanContainer{c, k.KVIOCH}
	k.KVIOCH <- KVIO{callType: KVGet, key: kvKey, value: value, chContainer: chContainer}
	return <-chContainer.c
}

func (k *kvssd) Delete(container, key string) error {
	kvKey := kvKeyName(container, key)
	c := make(chan error)
	chContainer := &chanContainer{c, k.KVIOCH}
	k.KVIOCH <- KVIO{callType: KVDelete, key: kvKey, chContainer: chContainer}
	return <-chContainer.c
}

func (k *kvssd) List() ([]string, error) {
	return nil, errDiskNotFound
}
