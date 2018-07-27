package cmd

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"strings"

	"github.com/klauspost/reedsolomon"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/hash"
	"github.com/tchap/go-patricia/patricia"
)

var sizePerKVEmul = 28 * 1024

type KVObject struct {
	Size         int
	Data, Parity int
	Modtime      time.Time
	Contents     []byte
}

type KVErasure struct {
	bucketName string
	disks      []KVAPI
	GatewayUnsupported
	trie *patricia.Trie
}

func newKVErasure(endpoints EndpointList) (*KVErasure, error) {
	kv := KVErasure{
		bucketName: os.Getenv("MINIO_BUCKET"),
		trie:       patricia.NewTrie(),
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
	originalObject := object
	if len(object) != 16 {
		object = getSHA256Hash([]byte(object))[:16]
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
	modTime := time.Now()
	for i := range k.disks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			o := KVObject{}
			o.Size = len(b)
			o.Data = dataDrives
			o.Parity = parityDrives
			o.Modtime = modTime
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
	k.trie.Insert(patricia.Prefix(originalObject), 1)
	return k.GetObjectInfo(ctx, bucket, object)
}

func (k *KVErasure) GetObjectInfo(ctx context.Context, bucket, object string) (objInfo ObjectInfo, err error) {
	if len(object) != 16 {
		object = getSHA256Hash([]byte(object))[:16]
	}
	parts := make([]KVObject, len(k.disks))
	errs := make([]error, len(k.disks))

	var wg sync.WaitGroup
	for i := range k.disks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var b []byte
			b, errs[i] = k.disks[i].Get(object)
			if errs[i] != nil {
				return
			}
			errs[i] = gob.NewDecoder(bytes.NewBuffer(b)).Decode(&parts[i])
			if errs[i] != nil && errs[i].Error() == "EOF" {
				errs[i] = errFileNotFound
			}
		}(i)
	}
	wg.Wait()
	part, err := kvQuorumPart(ctx, parts, errs)
	if err != nil {
		return objInfo, ObjectNotFound{}
	}

	objInfo.Bucket = bucket
	objInfo.Name = object
	objInfo.ModTime = part.Modtime
	objInfo.Size = int64(part.Size)
	return objInfo, nil
}

func (k *KVErasure) DeleteObject(ctx context.Context, bucket, object string) error {
	originalObject := object
	if len(object) != 16 {
		object = getSHA256Hash([]byte(object))[:16]
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
	wg.Wait()
	quorum := (len(k.disks) / 2) + 1
	if err := reduceWriteQuorumErrs(context.Background(), errs, nil, quorum); err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	k.trie.Delete(patricia.Prefix(originalObject))
	return nil
}

func kvQuorumPart(ctx context.Context, parts []KVObject, errs []error) (KVObject, error) {
	if err := reduceReadQuorumErrs(ctx, errs, nil, len(parts)/2); err != nil {
		return KVObject{}, err
	}

	modTimes := make([]time.Time, len(parts))
	for i := range parts {
		modTimes[i] = parts[i].Modtime
	}
	modTime, modTimeCount := commonTime(modTimes)
	if modTimeCount < len(parts)/2 {
		return KVObject{}, errXLReadQuorum
	}
	zero := time.Time{}
	if modTime == zero {
		return KVObject{}, ObjectNotFound{}
	}
	for i := range parts {
		if modTime == parts[i].Modtime {
			return parts[i], nil
		}
	}
	return KVObject{}, errFileNotFound
}

func (k *KVErasure) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string) (err error) {
	if len(object) != 16 {
		object = getSHA256Hash([]byte(object))[:16]
	}
	if startOffset != 0 {
		return NotImplemented{}
	}
	parts := make([]KVObject, len(k.disks))
	errs := make([]error, len(k.disks))

	var wg sync.WaitGroup
	for i := range k.disks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var b []byte
			b, errs[i] = k.disks[i].Get(object)
			if errs[i] != nil {
				return
			}
			errs[i] = gob.NewDecoder(bytes.NewBuffer(b)).Decode(&parts[i])
			if errs[i] != nil && errs[i].Error() == "EOF" {
				errs[i] = errFileNotFound
			}
		}(i)
	}
	wg.Wait()
	part, err := kvQuorumPart(ctx, parts, errs)
	if err != nil {
		return err
	}
	if err = reduceReadQuorumErrs(ctx, errs, nil, part.Data); err != nil {
		return err
	}

	contents := make([][]byte, len(k.disks))
	for i := range k.disks {
		if parts[i].Modtime == part.Modtime {
			contents[i] = parts[i].Contents
		}
	}

	erasure, err := reedsolomon.New(part.Data, part.Parity)
	if err != nil {
		logger.LogIf(ctx, err)
		return err
	}

	if err = erasure.ReconstructData(contents); err != nil {
		return err
	}

	remaining := part.Size
	for _, content := range contents[:part.Data] {
		if remaining < len(content) {
			content = content[:remaining]
		}
		writer.Write(content)
		remaining -= len(content)
	}

	return nil
}

func (k *KVErasure) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result ListObjectsInfo, err error) {
	var objects []string
	var commonPrefixes []string
	k.trie.Visit(func(objectByte patricia.Prefix, item patricia.Item) error {
		object := string(objectByte)
		if !strings.HasPrefix(object, prefix) {
			return nil
		}
		if strings.Compare(object, marker) <= 0 {
			return nil
		}
		if delimiter == "" {
			objects = append(objects, object)
			return nil
		}
		suffix := strings.TrimPrefix(object, prefix)
		i := strings.Index(suffix, slashSeparator)
		if i == -1 {
			objects = append(objects, object)
			return nil
		}
		fullPrefix := prefix + suffix[:i+1]
		for _, commonPrefix := range commonPrefixes {
			if commonPrefix == fullPrefix {
				return nil
			}
		}
		commonPrefixes = append(commonPrefixes, fullPrefix)
		return nil
	})
	for _, object := range objects {
		info, err := k.GetObjectInfo(ctx, bucket, object)
		if err != nil {
			continue
		}
		result.Objects = append(result.Objects, ObjectInfo{
			Bucket:  bucket,
			Name:    object,
			Size:    info.Size,
			ModTime: info.ModTime,
		})
	}
	result.Prefixes = commonPrefixes
	return result, nil
}
