package mqtt

import (
	"fmt"
	"io"
)

const MaxLenBytes = 4

func EncodeLength(w io.Writer, length int) (int, error) {
	bytes := 0
	for {
		encodedByte := byte(length % 128)
		length /= 128
		if length > 0 {
			// Set MSB
			encodedByte |= 128
		}
		if _, err := w.Write([]byte{encodedByte}); err != nil {
			return bytes, err
		}
		bytes++
		if length == 0 {
			break
		}

		if bytes >= MaxLenBytes {
			return bytes, fmt.Errorf("length too large to encode")
		}
	}
	return bytes, nil
}
