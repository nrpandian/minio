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

static struct kv_device_api* minio_kvs_open_device() {
    return kvs_open_device("/dev/kvemul", "./kvemul.conf");
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
}

static int32_t minio_kvs_get(struct kv_device_api *kvd,  void *key, int keyLen, void *value, int valueLen, uint64_t chPtr) {
    kvs_key kvskey = {key, keyLen};
    kvs_value kvsvalue = {value, valueLen, 0};
    const kvs_retrieve_context ret_ctx = { KVS_RETRIEVE_IDEMPOTENT, 0, (void*)chPtr, NULL };
    int i = kvs_retrieve_tuple(kvd, &kvskey, &kvsvalue, &ret_ctx);
    if (i) printf("kvs_retrieve_tuple retval = %d\n", i);
}

static int32_t minio_kvs_delete(struct kv_device_api *kvd,  void *key, int keyLen, uint64_t chPtr) {
    kvs_delete_context del_ctx = { KVS_DELETE_TUPLE, 0, NULL, NULL };
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
	"bytes"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"sync"
	"time"
	"unsafe"

	"encoding/gob"

	"strings"

	"github.com/klauspost/reedsolomon"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/hash"

	humanize "github.com/dustin/go-humanize"
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

func kvs_open_device() *C.struct_kv_device_api {
	return C.minio_kvs_open_device()
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
	for C.minio_kvs_get_ioevents(kvd, 1) == 0 {
		time.Sleep(10 * time.Millisecond)
	}

	err := <-errCh
	return err
}

func kvs_get(kvd *C.struct_kv_device_api, key string) ([]byte, error) {
	key = key + ":"
	keyBytes := []byte(key)
	valueLen := 2 * 1024 * 1024
	value := make([]byte, valueLen)
	errCh := make(chan error, 1)
	chContainer := chanContainer{errCh}
	C.minio_kvs_get(kvd, unsafe.Pointer(&keyBytes[0]), C.int(len(keyBytes)), unsafe.Pointer(&value[0]), C.int(len(value)), C.ulong(uintptr(unsafe.Pointer(&chContainer))))
	for C.minio_kvs_get_ioevents(kvd, 1) == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	err := <-errCh
	return value, err
}

func kvs_delete(kvd *C.struct_kv_device_api, key string) error {
	key = key + ":"
	keyBytes := []byte(key)
	errCh := make(chan error, 1)
	chContainer := chanContainer{errCh}
	C.minio_kvs_delete(kvd, unsafe.Pointer(&keyBytes[0]), C.int(len(keyBytes)), C.ulong(uintptr(unsafe.Pointer(&chContainer))))
	for C.minio_kvs_get_ioevents(kvd, 1) == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	err := <-errCh
	return err
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

var sizePerKVEmul = 1.9 * humanize.MiByte

type KVObject struct {
	Padding      []byte
	Size         int
	Data, Parity int
	Contents     []byte
}

func newKVEmul(endpoints EndpointList) (*KVEmul, error) {
	C.minio_kvs_init_env()
	emul := KVEmul{
		bucketName: globalSamsungBucket,
	}
	for _ = range endpoints {
		emul.kvd = append(emul.kvd, kvs_open_device())
	}
	return &emul, nil
}

type KVEmul struct {
	bucketName string
	kvd        []*C.struct_kv_device_api
	GatewayUnsupported
}

func (k *KVEmul) ListBuckets(ctx context.Context) (buckets []BucketInfo, err error) {
	return []BucketInfo{{k.bucketName, time.Now()}}, nil
}

func (k *KVEmul) GetBucketInfo(ctx context.Context, bucket string) (bucketInfo BucketInfo, err error) {
	if bucket != k.bucketName {
		return bucketInfo, BucketNotFound{Bucket: bucket}
	}
	return BucketInfo{k.bucketName, time.Now()}, nil
}

func (k *KVEmul) PutObject(ctx context.Context, bucket, object string, data *hash.Reader, metadata map[string]string) (objInfo ObjectInfo, err error) {
	if len(object) < 16 {
		return objInfo, NotImplemented{}
	}
	b, err := ioutil.ReadAll(data)
	if err != nil {
		return objInfo, ErrorRespToObjectError(err, bucket)
	}

	// No metadata is set, allocate a new one.
	if metadata == nil {
		metadata = make(map[string]string)
	}

	// Get parity and data drive count based on storage class metadata
	dataDrives, parityDrives := getRedundancyCount(metadata[amzStorageClass], len(k.kvd))

	if len(b) > dataDrives*int(sizePerKVEmul) {
		return objInfo, ObjectTooLarge{}
	}

	// we now know the number of blocks this object needs for data and parity.
	// writeQuorum is dataBlocks + 1
	writeQuorum := dataDrives + 1

	erasure, err := reedsolomon.New(dataDrives, parityDrives)
	if err != nil {
		logger.LogIf(ctx, err)
		return objInfo, err
	}

	encodedData, err := erasure.Split(b)
	if err != nil {
		logger.LogIf(ctx, err)
		return objInfo, err
	}
	if err = erasure.Encode(encodedData); err != nil {
		logger.LogIf(ctx, err)
		return objInfo, err
	}

	o := KVObject{}
	o.Padding = make([]byte, 64)
	o.Size = len(b)
	o.Data = dataDrives
	o.Parity = parityDrives

	errs := make([]error, len(k.kvd))
	var wg sync.WaitGroup
	for i := range k.kvd {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			o.Contents = encodedData[i]
			var w bytes.Buffer
			enc := gob.NewEncoder(&w)
			err := enc.Encode(o)
			if err != nil {
				errs[i] = err
				return
			}
			errs[i] = kvs_put(k.kvd[i], object, w.Bytes())
		}(i)
	}
	wg.Wait()
	if err = reduceWriteQuorumErrs(context.Background(), errs, nil, writeQuorum); err != nil {
		logger.LogIf(ctx, err)
		return objInfo, err
	}
	return k.GetObjectInfo(ctx, bucket, object)
}

func (k *KVEmul) GetObjectInfo(ctx context.Context, bucket, object string) (objInfo ObjectInfo, err error) {
	if len(object) < 16 {
		return objInfo, NotImplemented{}
	}
	b, err := kvs_get(k.kvd[0], object)
	if err != nil {
		return objInfo, ObjectNotFound{}
	}
	o := KVObject{}
	if err = gob.NewDecoder(bytes.NewBuffer(b)).Decode(&o); err != nil {
		return objInfo, err
	}

	objInfo.Bucket = bucket
	objInfo.Name = object
	objInfo.ModTime = time.Now()
	objInfo.Size = int64(o.Size)
	return objInfo, nil
}

func (k *KVEmul) DeleteObject(ctx context.Context, bucket, object string) error {
	if len(object) < 16 {
		return NotImplemented{}
	}
	errs := make([]error, len(k.kvd))
	var wg sync.WaitGroup
	for i, kvd := range k.kvd {
		wg.Add(1)
		go func(kvd *C.struct_kv_device_api) {
			defer wg.Done()
			errs[i] = kvs_delete(kvd, object)
		}(kvd)
	}
	wg.Done()
	quorum := (len(k.kvd) / 2) + 1
	if err := reduceWriteQuorumErrs(context.Background(), errs, nil, quorum); err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	return nil
}

func (k *KVEmul) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string) (err error) {
	if len(object) < 16 {
		return NotImplemented{}
	}
	if startOffset != 0 {
		return NotImplemented{}
	}
	b, err := kvs_get(k.kvd[0], object)
	if err != nil {
		return ObjectNotFound{}
	}
	o := KVObject{}
	if err = gob.NewDecoder(bytes.NewBuffer(b)).Decode(&o); err != nil {
		return err
	}
	shardSize := ceilFrac(int64(o.Size), int64(o.Data))
	remaining := o.Size
	for i := range k.kvd[:o.Data] {
		b, err = kvs_get(k.kvd[i], object)
		if err != nil {
			return ObjectNotFound{}
		}
		o = KVObject{}
		if err = gob.NewDecoder(bytes.NewBuffer(b)).Decode(&o); err != nil {
			return err
		}
		n := int(shardSize)
		if remaining < n {
			n = remaining
		}
		writer.Write(o.Contents[:n])
		remaining -= n
	}
	return nil
}

func (k *KVEmul) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result ListObjectsInfo, err error) {
	b, err := kvs_list(k.kvd[0])
	i := 0
	for ; i < len(b); i++ {
		if b[i] == 0 {
			break
		}
	}
	b = b[:i]
	for _, object := range strings.Split(string(b), ":") {
		if len(object) == 0 {
			continue
		}
		objInfo, err := k.GetObjectInfo(ctx, bucket, object)
		if err != nil {
			continue
		}
		result.Objects = append(result.Objects, objInfo)
	}
	return result, nil
}
