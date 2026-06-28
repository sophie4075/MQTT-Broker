package mqtt

import (
	"bytes"
	"fmt"
	"io"
)

const MaxLenBytes = 4

func EncodeLength(w io.Writer, length int) (int, error) {
	n := 0
	for {
		encodedByte := byte(length % 128)
		length /= 128
		if length > 0 {
			// Set MSB
			encodedByte |= 128
		}
		if _, err := w.Write([]byte{encodedByte}); err != nil {
			return n, err
		}
		n++
		if length == 0 {
			break
		}

		if n >= MaxLenBytes {
			return n, fmt.Errorf("length too large to encode")
		}
	}
	return n, nil
}

func WriteConnack(w io.Writer, hasSession bool, returnCode byte) error {
	_, err := w.Write([]byte{
		byte(CONNACK << 4),
		2,
		boolToByte(hasSession),
		returnCode,
	})
	return err
}

// WriteAck writes a generic ACK: PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK.
func WriteAck(w io.Writer, packetType PacketType, packetID uint16) error {
	firstByte := byte(packetType << 4)
	if packetType == PUBREL {
		firstByte |= 0x02
	}
	_, err := w.Write([]byte{firstByte, 2, byte(packetID >> 8), byte(packetID)})
	return err
}

func WriteSuback(w io.Writer, packetID uint16, returnCodes []byte) error {
	var buf bytes.Buffer
	buf.WriteByte(byte(SUBACK << 4))

	if _, err := EncodeLength(&buf, 2+len(returnCodes)); err != nil {
		return err
	}

	writeUint16(&buf, packetID)
	buf.Write(returnCodes)
	_, err := w.Write(buf.Bytes())
	return err
}

func WritePublish(w io.Writer, pkt *Publish) error {
	var body bytes.Buffer
	topicLen := len(pkt.TopicName)
	body.WriteByte(byte(topicLen >> 8))
	body.WriteByte(byte(topicLen))
	body.WriteString(pkt.TopicName)

	if pkt.Dup && pkt.QoS == 0 {
		return fmt.Errorf("DUP must be 0 for QoS 0 publish")
	}

	if pkt.QoS > 0 && pkt.PacketID == nil {
		return fmt.Errorf("publish with QoS %d requires a packet identifier", pkt.QoS)
	}

	if pkt.QoS > 0 {
		writeUint16(&body, *pkt.PacketID)
	}

	body.Write(pkt.Payload)

	firstByte := byte(PUBLISH << 4)
	if pkt.Dup {
		firstByte |= 0x08
	}
	firstByte |= byte((pkt.QoS & 0x03) << 1)
	if pkt.Retain {
		firstByte |= 0x01
	}
	var out bytes.Buffer
	out.WriteByte(firstByte)
	if _, err := EncodeLength(&out, body.Len()); err != nil {
		return err
	}
	out.Write(body.Bytes())
	_, err := w.Write(out.Bytes())
	return err
}

func WritePingresp(w io.Writer) error {
	_, err := w.Write([]byte{byte(PINGRESP << 4), 0})
	return err
}

func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

func writeUint16(buf *bytes.Buffer, v uint16) {
	buf.WriteByte(byte(v >> 8))
	buf.WriteByte(byte(v))
}
