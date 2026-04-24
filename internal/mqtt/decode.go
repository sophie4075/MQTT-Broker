package mqtt

import (
	"encoding/binary"
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

func ReadPacket(r io.Reader) (Packet, error) {
	headerByte := make([]byte, 1)
	if _, err := io.ReadFull(r, headerByte); err != nil {
		return nil, err
	}

	header := FixedHeader{
		PacketType: (headerByte[0] >> 4) & 0x0F,
		Dup:        (headerByte[0]>>3)&0x01 == 1,
		Qos:        (headerByte[0] >> 1) & 0x03,
		Retain:     headerByte[0]&0x01 == 1,
	}

	remaining, err := DecodeLength(r)
	if err != nil {
		return nil, err
	}
	header.RemainingLength = remaining

	body := make([]byte, remaining)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}

	switch header.PacketType {
	case CONNECT:
		return decodeConnect(header, body)
	case PUBLISH:
		return decodePublish(header, body)
	case SUBSCRIBE:
		return decodeSubscribe(header, body)
	case UNSUBSCRIBE:
		return decodeUnsubscribe(header, body)
	case PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK:
		// TODO
	case PINGREQ:
		// TODO
	case DISCONNECT:
		// TODO
	default:
		return nil, fmt.Errorf("unknown packet type: %d", header.PacketType)
	}
	return nil, nil
}

func decodeConnect(header FixedHeader, body []byte) (*Connect, error) {

	pkt := &Connect{Header: header}
	offset := 0

	protoLen := int(binary.BigEndian.Uint16(body[offset:]))
	offset += 2 + protoLen

	if offset+2 > len(body) {
		return nil, fmt.Errorf("malformed connect packet")
	}

	_ = body[offset]
	offset++

	flags := body[offset]
	offset++
	pkt.Bits.CleanSession = (flags>>1)&0x01 == 1
	pkt.Bits.WillFlag = (flags>>2)&0x01 == 1
	pkt.Bits.WillQos = (flags >> 3) & 0x03
	pkt.Bits.WillRetain = (flags>>5)&0x01 == 1
	pkt.Bits.PasswordFlag = (flags>>6)&0x01 == 1
	pkt.Bits.UsernameFlag = (flags>>7)&0x01 == 1

	pkt.Payload.KeepAlive = binary.BigEndian.Uint16(body[offset:])
	offset += 2

	pkt.Payload.ClientId, offset = readString(body, offset)

	if pkt.Bits.WillFlag {
		pkt.Payload.WillTopic, offset = readString(body, offset)
		pkt.Payload.WillMsg, offset = readBytes(body, offset)
	}
	if pkt.Bits.UsernameFlag {
		pkt.Payload.Username, offset = readString(body, offset)
	}
	if pkt.Bits.PasswordFlag {
		pkt.Payload.Password, offset = readBytes(body, offset)
	}

	return pkt, nil
}

func decodePublish(header FixedHeader, body []byte) (*Publish, error) {
	pkt := &Publish{Header: header}
	offset := 0

	pkt.TopicName, offset = readString(body, offset)
	topicLen := 2 + len(pkt.TopicName)

	messageLen := len(body)

	if header.Qos > 0 {
		pkt.PacketID = binary.BigEndian.Uint16(body[offset:])
		offset += 2
		messageLen -= 2
	}

	messageLen -= topicLen
	pkt.Payload = make([]byte, messageLen)
	copy(pkt.Payload, body[offset:offset+messageLen])

	return pkt, nil
}

func decodeSubscribe(header FixedHeader, body []byte) (*Subscribe, error) {
	pkt := &Subscribe{Header: header}
	offset := 0

	if len(body) < 2 {
		return nil, fmt.Errorf("subscribe packet too short")
	}
	pkt.PacketID = binary.BigEndian.Uint16(body[offset:])
	offset += 2

	for offset < len(body) {
		if offset+2 > len(body) {
			return nil, fmt.Errorf("truncated topic length")
		}
		topicLen := int(binary.BigEndian.Uint16(body[offset:]))
		offset += 2

		if offset+topicLen+1 > len(body) {
			return nil, fmt.Errorf("truncated topic or qos")
		}
		topic := string(body[offset : offset+topicLen])
		offset += topicLen

		qos := body[offset]
		offset++

		pkt.Topics = append(pkt.Topics, TopicSubscription{
			Topic: topic,
			Qos:   qos,
		})
	}
	return pkt, nil
}

func decodeUnsubscribe(header FixedHeader, body []byte) (*Unsubscribe, error) {
	pkt := &Unsubscribe{Header: header}
	offset := 0

	if len(body) < 2 {
		return nil, fmt.Errorf("unsubscribe packet too short")
	}
	pkt.PacketID = binary.BigEndian.Uint16(body[offset:])
	offset += 2

	for offset < len(body) {
		if offset+2 > len(body) {
			return nil, fmt.Errorf("truncated topic length")
		}
		topicLen := int(binary.BigEndian.Uint16(body[offset:]))
		offset += 2

		if offset+topicLen > len(body) {
			return nil, fmt.Errorf("truncated topic")
		}
		topic := string(body[offset : offset+topicLen])
		offset += topicLen

		offset++

		pkt.Topics = append(pkt.Topics, topic)
	}

	return pkt, nil
}

func readString(buf []byte, offset int) (string, int) {
	length := int(binary.BigEndian.Uint16(buf[offset:]))
	offset += 2
	s := string(buf[offset : offset+length])
	return s, offset + length
}

func readBytes(buf []byte, offset int) ([]byte, int) {
	length := int(binary.BigEndian.Uint16(buf[offset:]))
	offset += 2
	b := make([]byte, length)
	copy(b, buf[offset:offset+length])
	return b, offset + length
}
