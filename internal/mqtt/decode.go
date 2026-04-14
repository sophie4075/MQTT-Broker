package mqtt

import (
	"fmt"
	"io"
)

func DecodeLength(r io.Reader) (int, error) {
	multiplier := 1
	value := 0
	buf := make([]byte, 1)
	continueBit := true
	for continueBit {
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, err
		}
		value += int(buf[0]&127) * multiplier
		multiplier *= 128
		if multiplier > 128*128*128*128 {
			return 0, fmt.Errorf("malformed remaining length")
		}
		continueBit = buf[0]&128 != 0
	}
	return value, nil
}
