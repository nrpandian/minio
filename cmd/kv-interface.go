package cmd

import (
	"context"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/tchap/go-patricia/patricia"
)

var kvValueSize = func() int {
	size := os.Getenv("MINIO_KVSSD_VALUE_SIZE")
	if size == "" {
		return 28 * 1024
	}
	i, err := strconv.Atoi(size)
	if err != nil {
		logger.LogIf(context.Background(), err)
		return 28 * 1024
	}
	return i
}()

type KVPart struct {
	IDs        []string
	ETag       string
	Size       int64
	PartNumber int
}

type byKVPartNumber []KVPart

func (t byKVPartNumber) Len() int           { return len(t) }
func (t byKVPartNumber) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t byKVPartNumber) Less(i, j int) bool { return t[i].PartNumber < t[j].PartNumber }

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

func (k *KVNSEntry) AddPart(kvPart KVPart) {
	for i := range k.Parts {
		if k.Parts[i].PartNumber == kvPart.PartNumber {
			k.Parts[i] = kvPart
			return
		}
	}
	k.Parts = append(k.Parts, kvPart)
	sort.Sort(byKVPartNumber(k.Parts))
	k.Size += kvPart.Size
}

type KVListMap struct {
	listMap map[string]*patricia.Trie
	sync.Mutex
}

func (k *KVListMap) insertObject(bucket, object string) {
	k.Lock()
	defer k.Unlock()
	trie := k.listMap[bucket]
	if trie == nil {
		return
	}
	trie.Insert(patricia.Prefix(object), 1)
}

func (k *KVListMap) deleteObject(bucket, object string) {
	k.Lock()
	defer k.Unlock()
	trie := k.listMap[bucket]
	if trie == nil {
		return
	}
	trie.Delete(patricia.Prefix(object))
}

type KVAPI interface {
	Put(string, string, []byte) error
	Get(string, string, []byte) error
	Delete(string, string) error
	List() ([]string, error)
}
