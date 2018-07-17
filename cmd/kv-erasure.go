package cmd

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/hash"
)

var sizePerKVEmul = 28 * 1024

type KVObject struct {
	Size         int
	Data, Parity int
	Contents     []byte
}

type KVErasure struct {
	bucketName string
	disks      []KVAPI
	GatewayUnsupported
}

func newKVErasure(endpoints EndpointList) (*KVErasure, error) {
	kvs_init_env()
	kv := KVErasure{
		bucketName: globalSamsungBucket,
	}
	for _, endpoint := range endpoints {
		var disk KVAPI
		var err error
		if endpoint.IsLocal {
			disk, err = newKVSSD(endpoint.Path)
		} else {
			disk = newKVRPC(endpoint)
		}
		if err != nil {
			return nil, err
		}
		if disk == nil {
			return nil, fmt.Errorf("open failed on device %s", disk)
		}
		kv.disks = append(kv.disks, disk)
	}
	return &kv, nil
}

func (k *KVErasure) ListBuckets(ctx context.Context) (buckets []BucketInfo, err error) {
	return []BucketInfo{{k.bucketName, time.Now()}}, nil
}

func (k *KVErasure) GetBucketInfo(ctx context.Context, bucket string) (bucketInfo BucketInfo, err error) {
	if bucket != k.bucketName {
		return bucketInfo, BucketNotFound{Bucket: bucket}
	}
	return BucketInfo{k.bucketName, time.Now()}, nil
}

func (k *KVErasure) PutObject(ctx context.Context, bucket, object string, data *hash.Reader, metadata map[string]string) (objInfo ObjectInfo, err error) {
	if len(object) != 16 {
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
	dataDrives, parityDrives := getRedundancyCount(metadata[amzStorageClass], len(k.disks))

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

	errs := make([]error, len(k.disks))
	var wg sync.WaitGroup
	for i := range k.disks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			o := KVObject{}
			o.Size = len(b)
			o.Data = dataDrives
			o.Parity = parityDrives
			o.Contents = encodedData[i]
			var w bytes.Buffer
			enc := gob.NewEncoder(&w)
			err := enc.Encode(o)
			if err != nil {
				errs[i] = err
				return
			}
			if w.Len() > sizePerKVEmul {
				errs[i] = ObjectTooLarge{}
				return
			}
			w.Write(make([]byte, sizePerKVEmul-w.Len()))
			errs[i] = k.disks[i].Put(object, w.Bytes())
		}(i)
	}
	wg.Wait()
	if err = reduceWriteQuorumErrs(context.Background(), errs, nil, writeQuorum); err != nil {
		logger.LogIf(ctx, err)
		return objInfo, err
	}
	return k.GetObjectInfo(ctx, bucket, object)
}

func (k *KVErasure) GetObjectInfo(ctx context.Context, bucket, object string) (objInfo ObjectInfo, err error) {
	if len(object) < 16 {
		return objInfo, NotImplemented{}
	}
	b, err := k.disks[0].Get(object)
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

func (k *KVErasure) DeleteObject(ctx context.Context, bucket, object string) error {
	if len(object) < 16 {
		return NotImplemented{}
	}
	errs := make([]error, len(k.disks))
	var wg sync.WaitGroup
	for i := range k.disks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = k.disks[i].Delete(object)
		}(i)
	}
	wg.Done()
	quorum := (len(k.disks) / 2) + 1
	if err := reduceWriteQuorumErrs(context.Background(), errs, nil, quorum); err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	return nil
}

func (k *KVErasure) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string) (err error) {
	if len(object) < 16 {
		return NotImplemented{}
	}
	if startOffset != 0 {
		return NotImplemented{}
	}
	b, err := k.disks[0].Get(object)
	if err != nil {
		return ObjectNotFound{}
	}
	o := KVObject{}
	if err = gob.NewDecoder(bytes.NewBuffer(b)).Decode(&o); err != nil {
		return err
	}
	shardSize := ceilFrac(int64(o.Size), int64(o.Data))
	remaining := o.Size
	for i := range k.disks[:o.Data] {
		b, err = k.disks[i].Get(object)
		if err != nil {
			return ObjectNotFound{}
		}
		fmt.Println(string(b))
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

func (k *KVErasure) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result ListObjectsInfo, err error) {
	return result, NotImplemented{}
}
