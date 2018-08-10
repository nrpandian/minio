package cmd

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"strings"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/hash"
	"github.com/tchap/go-patricia/patricia"
)

const kvValueSize = 28 * 1024

type KVPart struct {
	IDs  []string
	ETag string
	Size int64
}

type KVNSEntry struct {
	Version                  string
	Bucket                   string
	Object                   string
	DataNumber, ParityNumber int
	Size                     int64
	ModTime                  time.Time
	Parts                    []KVPart
	ETag                     string
}

type KVErasureLayer struct {
	bucketName string
	disks      []KVAPI
	GatewayUnsupported
	trie *patricia.Trie
}

func newKVErasureLayer(endpoints EndpointList) (*KVErasureLayer, error) {
	bucketName := os.Getenv("MINIO_BUCKET")
	if bucketName == "" {
		bucketName = "default"
	}
	kv := KVErasureLayer{
		bucketName: bucketName,
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

func (k *KVErasureLayer) ListBuckets(ctx context.Context) (buckets []BucketInfo, err error) {
	return []BucketInfo{{k.bucketName, time.Now()}}, nil
}

func (k *KVErasureLayer) GetBucketInfo(ctx context.Context, bucket string) (bucketInfo BucketInfo, err error) {
	if bucket != k.bucketName {
		return bucketInfo, BucketNotFound{Bucket: bucket}
	}
	return BucketInfo{k.bucketName, time.Now()}, nil
}

func (k *KVErasureLayer) PutObject(ctx context.Context, bucket, object string, data *hash.Reader, metadata map[string]string) (objInfo ObjectInfo, err error) {
	dataDrives, parityDrives := getRedundancyCount(metadata[amzStorageClass], len(k.disks))
	erasure := newKVErasure(dataDrives, parityDrives)
	disks := make([]KVAPI, len(k.disks))
	copy(disks, k.disks)
	ids, n, err := erasure.Encode(ctx, disks, bucket, data)
	if err != nil {
		return objInfo, err
	}
	part := KVPart{ids, "", n}
	nsEntry := KVNSEntry{
		"1",
		bucket,
		object,
		dataDrives, parityDrives,
		n,
		time.Now(),
		[]KVPart{part},
		mustGetUUID(),
	}
	var buf bytes.Buffer
	err = gob.NewEncoder(&buf).Encode(nsEntry)
	if err != nil {
		return objInfo, err
	}
	if buf.Len() > kvValueSize {
		logger.LogIf(ctx, errUnexpected)
		return objInfo, errUnexpected
	}
	buf.Write(make([]byte, kvValueSize-buf.Len()))
	errs := make([]error, len(k.disks))
	var wg sync.WaitGroup
	for i := range k.disks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = k.disks[i].Put(bucket, object, buf.Bytes())
		}(i)
	}
	wg.Wait()
	if err = reduceWriteQuorumErrs(ctx, errs, nil, (len(k.disks)/2)+1); err != nil {
		return objInfo, err
	}
	objInfo.Name = object
	objInfo.Bucket = bucket
	objInfo.ModTime = nsEntry.ModTime
	objInfo.Size = nsEntry.Size
	objInfo.ETag = nsEntry.ETag
	k.trie.Insert(patricia.Prefix(object), 1)
	return objInfo, nil
}

func (k *KVErasureLayer) GetObjectInfo(ctx context.Context, bucket, object string) (objInfo ObjectInfo, err error) {
	entries := make([]KVNSEntry, len(k.disks))
	errs := make([]error, len(k.disks))

	var wg sync.WaitGroup
	for i := range k.disks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			b := make([]byte, kvValueSize)
			errs[i] = k.disks[i].Get(bucket, object, b)
			if errs[i] != nil {
				return
			}
			errs[i] = gob.NewDecoder(bytes.NewBuffer(b)).Decode(&entries[i])
			if errs[i] != nil && errs[i].Error() == "EOF" {
				errs[i] = errFileNotFound
			}
		}(i)
	}
	wg.Wait()
	entry, err := kvQuorumPart(ctx, entries, errs)
	if err != nil {
		logger.LogIf(ctx, err)
		return objInfo, toObjectErr(err, bucket, object)
	}

	objInfo.Bucket = bucket
	objInfo.Name = object
	objInfo.ModTime = entry.ModTime
	objInfo.Size = int64(entry.Size)
	objInfo.ETag = entry.ETag
	return objInfo, nil
}

func (k *KVErasureLayer) DeleteObject(ctx context.Context, bucket, object string) error {
	errs := make([]error, len(k.disks))
	var wg sync.WaitGroup
	for i := range k.disks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = k.disks[i].Delete(bucket, object)
		}(i)
	}
	wg.Wait()
	quorum := (len(k.disks) / 2) + 1
	if err := reduceWriteQuorumErrs(context.Background(), errs, nil, quorum); err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	k.trie.Delete(patricia.Prefix(object))
	return nil
}

func kvQuorumPart(ctx context.Context, entries []KVNSEntry, errs []error) (KVNSEntry, error) {
	if err := reduceReadQuorumErrs(ctx, errs, nil, len(entries)/2); err != nil {
		return KVNSEntry{}, err
	}

	modTimes := make([]time.Time, len(entries))
	for i := range entries {
		modTimes[i] = entries[i].ModTime
	}
	modTime, modTimeCount := commonTime(modTimes)
	if modTimeCount < len(entries)/2 {
		return KVNSEntry{}, errXLReadQuorum
	}
	zero := time.Time{}
	if modTime == zero {
		return KVNSEntry{}, ObjectNotFound{}
	}
	for i := range entries {
		if modTime == entries[i].ModTime {
			return entries[i], nil
		}
	}
	return KVNSEntry{}, errFileNotFound
}

func (k *KVErasureLayer) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string) (err error) {
	if startOffset != 0 {
		return NotImplemented{}
	}
	entries := make([]KVNSEntry, len(k.disks))
	errs := make([]error, len(k.disks))

	var wg sync.WaitGroup
	for i := range k.disks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			b := make([]byte, kvValueSize)
			errs[i] = k.disks[i].Get(bucket, object, b)
			if errs[i] != nil {
				return
			}
			errs[i] = gob.NewDecoder(bytes.NewBuffer(b)).Decode(&entries[i])
			if errs[i] != nil && errs[i].Error() == "EOF" {
				errs[i] = errFileNotFound
			}
		}(i)
	}
	wg.Wait()
	entry, err := kvQuorumPart(ctx, entries, errs)
	if err != nil {
		return err
	}
	if entry.Size != length {
		return NotImplemented{}
	}
	disks := make([]KVAPI, len(k.disks))
	copy(disks, k.disks)
	erasure := newKVErasure(entry.DataNumber, entry.ParityNumber)
	return erasure.Decode(ctx, disks, bucket, entry.Parts[0].IDs, entry.Size, entry.DataNumber, writer)
}

func (k *KVErasureLayer) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result ListObjectsInfo, err error) {
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
