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

static void on_io_complete(void *private1, void *private2, kvs_result_t op_result) {
  const char *errStr = NULL;
  if (op_result.result != KVS_SUCCESS) {
    errStr = "error in callback";
  }
  on_io_complete_callback(private1, errStr);
}

static void minio_kvs_init_env() {
  aio_context aiocontext;
  aiocontext.num_aiothreads_per_device = 1;
  aiocontext.num_devices_per_aiothread = 1;
  aiocontext.queuedepth = 128;
  aiocontext.io_complete = on_io_complete;

  int ret = kvs_init_env_ex(0, &aiocontext, 0);
  if (ret != KVS_SUCCESS) {
    printf("kvs_init_env() returned error");
  }
}

static int minio_kvs_open_device(char *device) {
  return kvs_open_device(device, 8);
}

static void minio_kvs_close_device(int devid) {
  kvs_close_device(devid);
}

static int minio_kvs_create_container(int devid) {
  kvs_group_by group;
  group.index_count = 3;
  group.key_index[0] = 0;
  group.key_index[1] = 1;
  group.key_index[2] = 2;
  group.ordered = KVS_GROUP_ORDER_NONE;
  return kvs_create_container(devid, "default", KVS_KEY_STRING, 0, &group);
}

static int32_t minio_kvs_put(int devid, int containerid, void *key, int keyLen, void *value, int valueLen, uint64_t chPtr) {
    kvs_key kvskey = {key, 16};
    kvs_value kvsvalue = {value, valueLen, 0};
    const kvs_store_context put_ctx = { KVS_STORE_POST, 0, (void*)chPtr, NULL};

//printf("PUT: key:%16x%16x len:%d vlen:%d\n", *(int*)key, *(((int*)key)+1), keyLen, valueLen);
    int i = kvs_store_tuple(devid, containerid, &kvskey, &kvsvalue, &put_ctx);
    if (i) printf("kvs_store_tuple retval = %d\n", i);
    return i;
}

static int32_t minio_kvs_get(int devid, int containerid, void *key, int keyLen, void *value, int valueLen, uint64_t chPtr) {
    kvs_key kvskey = {key, 16};
    kvs_value kvsvalue = {value, valueLen, 0};
//printf("GET: key:%16x%16x len:%d vlen:%d\n", *(int*)key, *(((int*)key)+1), keyLen, valueLen);
    const kvs_retrieve_context ret_ctx = { KVS_RETRIEVE_IDEMPOTENT, 0, (void*)chPtr, NULL };
    int i = kvs_retrieve_tuple(devid, containerid, &kvskey, &kvsvalue, &ret_ctx);
    if (i) printf("kvs_retrieve_tuple retval = %d\n", i);
    return i;
}

static int32_t minio_kvs_delete(int devid, int containerid,  void *key, int keyLen, uint64_t chPtr) {
    kvs_delete_context del_ctx = {KVS_DELETE_TUPLE};
    del_ctx.private1 = (void*) chPtr;

//    kvs_delete_context del_ctx = { KVS_DELETE_TUPLE, 0, (void*)chPtr, NULL };
    kvs_key kvskey = {key, 16};
//printf("DEL: key:%16x%16x len:%d\n", *(int*)key, *(((int*)key)+1), keyLen);
    int i = kvs_delete_tuple(devid, containerid, &kvskey, &del_ctx);
    if (i) printf("kvs_delete_tuple retval = %d\n", i);
    return i;
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
	KVIOCH <- KVIO{callType: KVCallback, chContainer: chContainer, err: err}
}

type KVCallType int

const (
	KVPut KVCallType = iota
	KVGet
	KVDelete
	KVCallback
)

type chanContainer struct {
	c chan error
}

type KVIO struct {
	callType    KVCallType
	key         []byte
	value       unsafe.Pointer
	chContainer *chanContainer
	err         error
}

var KVIOCH chan KVIO

func (k *kvssd) kv_io() {
	runtime.LockOSThread()
	chanContainerMap := make(map[*chanContainer]bool)
	for {
		kvio := <-KVIOCH
		switch kvio.callType {
		case KVPut:
			chanContainerMap[kvio.chContainer] = true
			C.minio_kvs_put(k.devid, k.containerid, unsafe.Pointer(&kvio.key[0]), C.int(len(kvio.key)), kvio.value, C.int(28*1024), C.ulong(uintptr(unsafe.Pointer(kvio.chContainer))))
		case KVGet:
			chanContainerMap[kvio.chContainer] = true
			//C.minio_kvs_get(k.devid, k.containerid, unsafe.Pointer(&kvio.key[0]), C.int(len(kvio.key)), kvio.vptr, C.int(28*1024), C.ulong(uintptr(unsafe.Pointer(kvio.chContainer))))
			C.minio_kvs_get(k.devid, k.containerid, unsafe.Pointer(&kvio.key[0]), C.int(len(kvio.key)), kvio.value, C.int(28*1024), C.ulong(uintptr(unsafe.Pointer(kvio.chContainer))))
		case KVDelete:
			chanContainerMap[kvio.chContainer] = true
			C.minio_kvs_delete(k.devid, k.containerid, unsafe.Pointer(&kvio.key[0]), C.int(len(kvio.key)), C.ulong(uintptr(unsafe.Pointer(kvio.chContainer))))
		case KVCallback:
			kvio.chContainer.c <- kvio.err
			delete(chanContainerMap, kvio.chContainer)
		}
	}
}

func kvs_init_env() {
	C.minio_kvs_init_env()
}

func kvs_open_device(device string) _Ctype_int {
	deviceCstr := C.CString(device)
	return C.minio_kvs_open_device(deviceCstr)
}

// func kvs_close_device() {
// 	C.minio_kvs_close_device(kvd)
// }

type kvssd struct {
	device      string
	devid       _Ctype_int
	containerid _Ctype_int
        pool_name string
        task_pool unsafe.Pointer
        numa_attached _Ctype_int
}

func newKVSSD(device string) (KVAPI, error, unsafe.Pointer) {
	if strings.HasPrefix(device, "/dev/xfs") {
	        return nil, errUnexpected, nil
	}
	if strings.HasPrefix(device, "/dev/kvemul") {
		device = "/dev/kvemul"
	}
	KVIOCH = make(chan KVIO, 1024)
//	fmt.Println("calling kvs_open_device", device)
	devid := kvs_open_device(device)
//	fmt.Println("kvs_open_device() returned", devid)
	if devid < 0 {
		return nil, errDiskNotFound, nil
	}
	containerid := C.minio_kvs_create_container(devid)
	if containerid < 0 {
		fmt.Printf("container id < 0")
		return nil, errUnexpected, nil
	}

        pool_name := fmt.Sprintf("task_pool_%d_%d_%d", devid, containerid, -1)
        task_pool := C._kvs_mempool_create(C.CString(pool_name), C.ulong(1024), C.ulong(28 * 1024),  C.int(-1));
        if (task_pool == nil) {
            fmt.Println("task_pool is nil")
            return nil, errUnexpected, nil
        }

        k := &kvssd{device, devid, containerid, pool_name, task_pool, -1}
        
	go k.kv_io()
	
	return k, nil, task_pool
}

func kvKeyName(container, key string) []byte {
	return getSHA256Sum([]byte(pathJoin(container, key)))[:16]
}

func (k *kvssd) Put(container, key string, value unsafe.Pointer) error {
	kvKey := kvKeyName(container, key)
	c := make(chan error)
	chContainer := &chanContainer{c}
	KVIOCH <- KVIO{callType: KVPut, key: kvKey, value: value, chContainer: chContainer}
	return <-chContainer.c
}

func (k *kvssd) Get(container, key string, value unsafe.Pointer) error {
	kvKey := kvKeyName(container, key)
	c := make(chan error)
	chContainer := &chanContainer{c}
	KVIOCH <- KVIO{callType: KVGet, key: kvKey, value: value, chContainer: chContainer}
	return <-chContainer.c
}

func (k *kvssd) Delete(container, key string) error {
	kvKey := kvKeyName(container, key)
	c := make(chan error)
	chContainer := &chanContainer{c}
	KVIOCH <- KVIO{callType: KVDelete, key: kvKey, chContainer: chContainer}
	return <-chContainer.c
}

func (k *kvssd) List() ([]string, error) {
	return nil, errDiskNotFound
}
