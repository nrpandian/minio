package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/hash"
	"github.com/tchap/go-patricia/patricia"
)

type KV struct {
	device     string
	bucketName string
	disk       KVAPI
	GatewayUnsupported
	trie *patricia.Trie
}

func newKV(device string) (*KV, error) {
	bucketName := os.Getenv("MINIO_BUCKET")
	if bucketName == "" {
		bucketName = "default"
	}
	disk, err := newKVSSD(device)
	if err != nil {
		return nil, err
	}
	return &KV{device: device, bucketName: bucketName, disk: disk, trie: patricia.NewTrie()}, nil
}

func (k *KV) ListBuckets(ctx context.Context) (buckets []BucketInfo, err error) {
	return []BucketInfo{{k.bucketName, time.Now()}}, nil
}

func (k *KV) GetBucketInfo(ctx context.Context, bucket string) (bucketInfo BucketInfo, err error) {
	if bucket != k.bucketName {
		return bucketInfo, BucketNotFound{Bucket: bucket}
	}
	return BucketInfo{k.bucketName, time.Now()}, nil
}

func (k *KV) WriteStream(ctx context.Context, disk KVAPI, bucket string, reader io.Reader) ([]string, int64, error) {
	buf := make([]byte, kvValueSize)
	var ids []string
	var total int64
	for {
		n, err := io.ReadFull(reader, buf)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			logger.LogIf(ctx, err)
			return nil, 0, err
		}
		eof := err == io.EOF || err == io.ErrUnexpectedEOF
		if n == 0 {
			break
		}
		uuid := mustGetUUID()
		err = disk.Put(bucket, uuid, buf)
		if err != nil {
			return nil, 0, err
		}
		ids = append(ids, uuid)
		total += int64(n)
		if eof {
			break
		}
	}
	return ids, total, nil
}

func (k *KV) ReadStream(ctx context.Context, bucket string, ids []string, length int64, writer io.Writer) error {
	buf := make([]byte, kvValueSize)
	for _, id := range ids {
		err := k.disk.Get(bucket, id, buf)
		if err != nil {
			return err
		}
		if length < int64(len(buf)) {
			buf = buf[:length]
		}
		writer.Write(buf)
		length -= int64(len(buf))
	}
	return nil
}

func (k *KV) PutObject(ctx context.Context, bucket, object string, data *hash.Reader, metadata map[string]string) (objInfo ObjectInfo, err error) {
	ids, n, err := k.WriteStream(ctx, k.disk, bucket, data)
	if err != nil {
		return objInfo, err
	}
	part := KVPart{ids, "", n, 1}
	nsEntry := KVNSEntry{
		"1",
		bucket,
		object,
		0, 0,
		n,
		time.Now(),
		[]KVPart{part},
		mustGetUUID(),
	}
	var buf bytes.Buffer
	err = json.NewEncoder(&buf).Encode(nsEntry)
	// err = gob.NewEncoder(&buf).Encode(nsEntry)
	if err != nil {
		return objInfo, err
	}
	if buf.Len() > kvValueSize {
		logger.LogIf(ctx, errUnexpected)
		return objInfo, errUnexpected
	}
	buf.Write(make([]byte, kvValueSize-buf.Len()))
	err = k.disk.Put(k.bucketName, object, buf.Bytes())
	if err != nil {
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

func (k *KV) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string) (err error) {
	if startOffset != 0 {
		return NotImplemented{}
	}
	b := make([]byte, kvValueSize)
	err = k.disk.Get(bucket, object, b)
	if err != nil {
		return err
	}
	var entry KVNSEntry
	err = json.NewDecoder(bytes.NewBuffer(b)).Decode(&entry)
	// errs[i] = gob.NewDecoder(bytes.NewBuffer(b)).Decode(&entries[i])
	if err != nil {
		err = errFileNotFound
	}
	for _, part := range entry.Parts {
		err = k.ReadStream(ctx, bucket, part.IDs, part.Size, writer)
		if err != nil {
			return err
		}
	}
	return nil
}

func (k *KV) GetObjectInfo(ctx context.Context, bucket, object string) (objInfo ObjectInfo, err error) {
	b := make([]byte, kvValueSize)
	err = k.disk.Get(bucket, object, b)
	if err != nil {
		return objInfo, ObjectNotFound{}
	}
	var entry KVNSEntry
	err = json.NewDecoder(bytes.NewBuffer(b)).Decode(&entry)
	// errs[i] = gob.NewDecoder(bytes.NewBuffer(b)).Decode(&entries[i])
	if err != nil {
		return objInfo, ObjectNotFound{}
	}
	objInfo.Bucket = bucket
	objInfo.Name = object
	objInfo.ModTime = entry.ModTime
	objInfo.Size = int64(entry.Size)
	objInfo.ETag = entry.ETag
	return objInfo, nil
}

func (k *KV) DeleteObject(ctx context.Context, bucket, object string) error {
	var entry KVNSEntry
	b := make([]byte, kvValueSize)
	err := k.disk.Get(bucket, object, b)
	if err != nil {
		return err
	}
	err = json.NewDecoder(bytes.NewBuffer(b)).Decode(&entry)
	// errs[i] = gob.NewDecoder(bytes.NewBuffer(b)).Decode(&entries[i])
	if err != nil {
		return errFileNotFound
	}

	var blockIDs []string
	for _, part := range entry.Parts {
		blockIDs = append(blockIDs, part.IDs...)
	}

	for _, blockID := range blockIDs {
		k.disk.Delete(bucket, blockID)
	}
	err = k.disk.Delete(bucket, object)
	k.trie.Delete(patricia.Prefix(object))
	return err
}

func (k *KV) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result ListObjectsInfo, err error) {
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
