package cmd

import (
	"fmt"
	"io"
)

type stripeEncoder struct {
	DataNum int
}

func (s *stripeEncoder) Encode(shards [][]byte) error {
	return nil
}

func (s *stripeEncoder) Verify(shards [][]byte) (bool, error) {
	return true, nil
}

func (s *stripeEncoder) Reconstruct(shards [][]byte) error {
	return fmt.Errorf("reconstruction not supported by stripe encoder")
}

func (s *stripeEncoder) ReconstructData(shards [][]byte) error {
	return fmt.Errorf("reconstruction not supported by stripe encoder")
}

func (s *stripeEncoder) Update(shards [][]byte, newDatashards [][]byte) error {
	return fmt.Errorf("reconstruction not supported by stripe encoder")
}

func (s *stripeEncoder) Split(data []byte) ([][]byte, error) {
	if len(data)%s.DataNum != 0 {
		return nil, fmt.Errorf("data can not be split")
	}
	perShard := len(data) / s.DataNum
	bufs := make([][]byte, s.DataNum)
	for i := range bufs {
		bufs[i] = data[:perShard]
		data = data[perShard:]
	}
	return bufs, nil
}

func (s *stripeEncoder) Join(dst io.Writer, shards [][]byte, outSize int) error {
	return fmt.Errorf("reconstruction not supported by stripe encoder")
}

func newStripeEncoder(dataNum int) *stripeEncoder {
	return &stripeEncoder{dataNum}
}
