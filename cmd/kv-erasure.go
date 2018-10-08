package cmd

import (
	"context"
	"io"
	"sync"

	"github.com/klauspost/reedsolomon"
	"github.com/minio/minio/cmd/logger"
)

type KVErasure struct {
	encoder            reedsolomon.Encoder
	DataNum, ParityNum int
	BlockSize          int
}

func (k *KVErasure) EncodeData(ctx context.Context, data []byte) ([][]byte, error) {
	if len(data) == 0 {
		logger.LogIf(ctx, errUnexpected)
		return nil, errUnexpected
	}
	encoded, err := k.encoder.Split(data)
	if err != nil {
		logger.LogIf(ctx, err)
		return nil, err
	}
	if err = k.encoder.Encode(encoded); err != nil {
		logger.LogIf(ctx, err)
		return nil, err
	}
	return encoded, nil
}

func (k *KVErasure) DecodeData(ctx context.Context, blocks [][]byte) error {
	needsReconstruction := false
	for _, b := range blocks[:k.DataNum] {
		if b == nil {
			needsReconstruction = true
			break
		}
	}
	if !needsReconstruction {
		return nil
	}
	if err := k.encoder.ReconstructData(blocks); err != nil {
		return err
	}
	return nil
}

func newKVErasure(dataNum, parityNum int) *KVErasure {
	var encoder reedsolomon.Encoder
	var err error
	if parityNum == 0 {
		encoder = newStripeEncoder(dataNum)
	} else {
		encoder, err = reedsolomon.New(dataNum, parityNum)
		if err != nil {
			panic(err.Error())
		}
	}
	blockSize := dataNum * kvValueSize
	return &KVErasure{encoder, dataNum, parityNum, blockSize}
}

type kvParallelWriter struct {
	disks       []KVAPI
	writeQuorum int
	bucket      string
}

func (p *kvParallelWriter) Put(ctx context.Context, key string, blocks [][]byte) error {
	var wg sync.WaitGroup
	errs := make([]error, len(p.disks))
	for i := range p.disks {
		if p.disks[i] == nil {
			errs[i] = errDiskNotFound
			continue
		}

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = p.disks[i].Put(p.bucket, key, blocks[i])
			if errs[i] != nil {
				p.disks[i] = nil
			}
		}(i)
	}
	wg.Wait()

	return reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, p.writeQuorum)
}

func (k *KVErasure) Encode(ctx context.Context, disks []KVAPI, bucket string, reader io.Reader) ([]string, int64, error) {
	p := &kvParallelWriter{disks, k.DataNum, bucket}
	buf := kvAllocBloc()
	defer kvFreeBlock(buf)
	var ids []string
	var total int64
	for {
		var blocks [][]byte
		n, err := io.ReadFull(reader, buf)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			logger.LogIf(ctx, err)
			return nil, 0, err
		}
		eof := err == io.EOF || err == io.ErrUnexpectedEOF
		if n == 0 {
			break
		}

		blocks, err = k.EncodeData(ctx, buf)
		if err != nil {
			logger.LogIf(ctx, err)
			return nil, 0, err
		}
		uuid := mustGetUUID()
		err = p.Put(ctx, uuid, blocks)
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

type kvParallelReader struct {
	disks      []KVAPI
	bucket     string
	ids        []string
	currentId  int
	readQuorum int
	blocks     [][]byte
}

func (k *kvParallelReader) Read(ctx context.Context) ([][]byte, error) {
	blocks := make([][]byte, len(k.disks))
	errs := make([]error, len(k.disks))
	if len(k.ids) == 0 {
		return blocks, nil
	}
	var wg sync.WaitGroup
	for i := range k.disks {
		errs[i] = errDiskNotFound
	}
	for i := range k.disks[:k.readQuorum] {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = k.disks[i].Get(k.bucket, k.ids[k.currentId], k.blocks[i])
			if errs[i] != nil && errs[i].Error() == "EOF" {
				errs[i] = errFileNotFound
			}
			errs[i] = nil
			if errs[i] == nil {
				blocks[i] = k.blocks[i]
			}
		}(i)
	}
	wg.Wait()
	err := reduceReadQuorumErrs(ctx, errs, objectOpIgnoredErrs, k.readQuorum)
	k.currentId++
	return blocks, err
}

func newKVParallelReader(disks []KVAPI, bucket string, ids []string, offset int64, readQuorum int, blocks [][]byte) *kvParallelReader {
	blockSize := int64(readQuorum * kvValueSize)
	currentId := offset / blockSize
	return &kvParallelReader{disks, bucket, ids, int(currentId), readQuorum, blocks}
}

func (k *KVErasure) Decode(ctx context.Context, disks []KVAPI, bucket string, ids []string, offset, length int64, readQuorum int, writer io.Writer) error {
	if length == 0 {
		return nil
	}
	blocks := make([][]byte, len(disks))
	for i := range blocks {
		blocks[i] = kvAlloc()
		defer kvFree(blocks[i])
	}

	blockSize := int64(kvValueSize * len(globalEndpoints))
	reader := newKVParallelReader(disks, bucket, ids, offset, readQuorum, blocks)

	startBlock := offset / blockSize
	endBlock := (offset + length) / blockSize

	var bytesWritten int64
	for block := startBlock; block <= endBlock; block++ {
		var blockOffset, blockLength int64
		switch {
		case startBlock == endBlock:
			blockOffset = offset % blockSize
			blockLength = length
		case block == startBlock:
			blockOffset = offset % blockSize
			blockLength = blockSize - blockOffset
		case block == endBlock:
			blockOffset = 0
			blockLength = (offset + length) % blockSize
		default:
			blockOffset = 0
			blockLength = blockSize
		}
		if blockLength == 0 {
			break
		}
		bufs, err := reader.Read(ctx)
		if err != nil {
			logger.LogIf(ctx, err)
			return err
		}
		if err = k.DecodeData(ctx, blocks); err != nil {
			logger.LogIf(ctx, err)
			return err
		}
		n, err := writeDataBlocks(ctx, writer, bufs, k.DataNum, blockOffset, blockLength)
		if err != nil {
			return err
		}
		bytesWritten += n
	}
	if bytesWritten != length {
		logger.LogIf(ctx, errLessData)
		return errLessData
	}
	return nil

}
