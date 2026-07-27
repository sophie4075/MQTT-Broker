package mqtt

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

// reservedFlags defines the expected value of the lower 4 bits (bits 3–0)
// of the first byte in the MQTT Fixed Header for each Control Packet type.
//
// These bits are packet-type specific and may either:
//   - be fixed to a defined value (as specified by the MQTT specification), or
//   - be reserved for future use.
//
// The receiver MUST validate these flags when parsing an incoming packet.
// If an invalid value is detected, the connection MUST be terminated
// according to the MQTT specification.
//
// see MQTT Version 3.1.1, Section 2.2.2 (Fixed Header Flags)
var reservedFlags = map[PacketType]byte{
	CONNECT:     0x00,
	CONNACK:     0x00,
	PUBACK:      0x00,
	PUBREC:      0x00,
	PUBREL:      0x02,
	PUBCOMP:     0x00,
	SUBSCRIBE:   0x02,
	SUBACK:      0x00,
	UNSUBSCRIBE: 0x02,
	UNSUBACK:    0x00,
	PINGREQ:     0x00,
	PINGRESP:    0x00,
	DISCONNECT:  0x00,
}

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
		PacketType: PacketType(headerByte[0] >> 4),
		Flags:      headerByte[0] & 0x0F,
	}

	if err := validateFlags(header); err != nil {
		return nil, err
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
		return decodeConnect(body)
	case PUBLISH:
		return decodePublish(header, body)
	case SUBSCRIBE:
		return decodeSubscribe(body)
	case UNSUBSCRIBE:
		return decodeUnsubscribe(body)
	case PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK:
		return decodeAck(header, body)
	case PINGREQ:
		return &Pingreq{}, nil
	case PINGRESP:
		return &Pingresp{}, nil
	case DISCONNECT:
		return &Disconnect{}, nil
	default:
		return nil, fmt.Errorf("unknown packet type: %d", header.PacketType)
	}
}

func decodeConnect(body []byte) (*Connect, error) {
	var err error
	var protoLen uint16
	pkt := &Connect{}
	offset := 0

	// Sticky-error wrappers: once err is set, further reads do nothing,
	// so the optional payload fields below stay free of repeated err checks.
	readStr := func(dst *string) {
		if err != nil {
			return
		}
		*dst, offset, err = readString(body, offset)
	}
	readBin := func(dst *[]byte) {
		if err != nil {
			return
		}
		*dst, offset, err = readBytes(body, offset)
	}

	// Protocol Name: length-prefixed, content is skipped (not stored).
	protoLen, offset, err = readUint16(body, offset)
	if err != nil {
		return nil, fmt.Errorf("malformed connect: %w", err)
	}
	offset += int(protoLen)

	// Protocol Level (1 byte) + Connect Flags (1 byte) must still fit.
	if offset+2 > len(body) {
		return nil, fmt.Errorf("malformed connect packet")
	}
	offset++ // Ignore Protocol Level

	flags := body[offset]
	offset++
	if flags&0x01 != 0 {
		return nil, fmt.Errorf("connect reserved flag must be 0")
	}
	pkt.Flags.CleanSession = (flags>>1)&0x01 == 1
	pkt.Flags.WillFlag = (flags>>2)&0x01 == 1
	pkt.Flags.WillQoS = QoS((flags >> 3) & 0x03)
	pkt.Flags.WillRetain = (flags>>5)&0x01 == 1
	pkt.Flags.PasswordFlag = (flags>>6)&0x01 == 1
	pkt.Flags.UsernameFlag = (flags>>7)&0x01 == 1

	if err := validateConnectFlags(pkt.Flags); err != nil {
		return nil, err
	}

	pkt.KeepAlive, offset, err = readUint16(body, offset)
	if err != nil {
		return nil, err
	}

	readStr(&pkt.Payload.ClientID)

	if pkt.Flags.WillFlag {
		readStr(&pkt.Payload.WillTopic)
		readBin(&pkt.Payload.WillMsg)
	}
	if pkt.Flags.UsernameFlag {
		readStr(&pkt.Payload.Username)
	}
	if pkt.Flags.PasswordFlag {
		readBin(&pkt.Payload.Password)
	}

	if err != nil {
		return nil, err
	}

	return pkt, nil
}

func decodePublish(header FixedHeader, body []byte) (*Publish, error) {
	pkt := &Publish{
		Dup:    (header.Flags>>3)&0x01 == 1,
		QoS:    QoS((header.Flags >> 1) & 0x03),
		Retain: header.Flags&0x01 == 1,
	}
	if pkt.QoS == 3 {
		return nil, fmt.Errorf("invalid QoS 3 in PUBLISH")
	}
	offset := 0

	var err error
	pkt.TopicName, offset, err = readString(body, offset)
	if err != nil {
		return nil, err
	}

	err = validateTopicName(pkt.TopicName)
	if err != nil {
		return nil, err
	}

	// Packet Identifier only present for QoS > 0.
	if pkt.QoS > 0 {
		var id uint16
		id, offset, err = readUint16(body, offset)
		if err != nil {
			return nil, fmt.Errorf("missing packet identifier: %w", err)
		}
		pkt.PacketID = &id
	}

	pkt.Payload = make([]byte, len(body)-offset)
	copy(pkt.Payload, body[offset:])

	return pkt, nil
}

func decodeSubscribe(body []byte) (*Subscribe, error) {
	pkt := &Subscribe{}
	offset := 0

	var err error
	pkt.PacketID, offset, err = readUint16(body, offset)
	if err != nil {
		return nil, fmt.Errorf("subscribe packet too short: %w", err)
	}

	for offset < len(body) {
		var topic string
		topic, offset, err = readString(body, offset)
		if err != nil {
			return nil, err
		}

		if offset+1 > len(body) {
			return nil, fmt.Errorf("truncated qos")
		}
		qos := body[offset]
		offset++

		pkt.Topics = append(pkt.Topics, TopicSubscription{
			Topic: topic,
			QoS:   QoS(qos),
		})
	}

	// MUST contain at least one Topic Filter.
	if len(pkt.Topics) == 0 {
		return nil, fmt.Errorf("subscribe with no topics")
	}
	return pkt, nil
}

func decodeUnsubscribe(body []byte) (*Unsubscribe, error) {
	pkt := &Unsubscribe{}
	offset := 0

	var err error
	pkt.PacketID, offset, err = readUint16(body, offset)
	if err != nil {
		return nil, fmt.Errorf("unsubscribe packet too short: %w", err)
	}

	for offset < len(body) {
		var topic string
		topic, offset, err = readString(body, offset)
		if err != nil {
			return nil, err
		}
		pkt.Topics = append(pkt.Topics, topic)
	}

	// MUST contain at least one Topic Filter.
	if len(pkt.Topics) == 0 {
		return nil, fmt.Errorf("unsubscribe with no topics")
	}
	return pkt, nil
}

func decodeAck(header FixedHeader, body []byte) (*Ack, error) {
	id, _, err := readUint16(body, 0)
	if err != nil {
		return nil, fmt.Errorf("ack packet too short: %w", err)
	}
	return &Ack{
		Kind:     header.PacketType,
		PacketID: id,
	}, nil
}

// readUint16 reads 2 big-endian bytes and returns the new offset.
func readUint16(buf []byte, offset int) (uint16, int, error) {
	if offset+2 > len(buf) {
		return 0, offset, fmt.Errorf("truncated uint16")
	}
	return binary.BigEndian.Uint16(buf[offset:]), offset + 2, nil
}

// readString reads a length-prefixed UTF-8 string and validates it.
func readString(buf []byte, offset int) (string, int, error) {
	length, offset, err := readUint16(buf, offset)
	if err != nil {
		return "", offset, fmt.Errorf("truncated string length: %w", err)
	}
	if offset+int(length) > len(buf) {
		return "", offset, fmt.Errorf("truncated string")
	}
	s := string(buf[offset : offset+int(length)])
	if err := validateUTF8(s); err != nil {
		return "", offset, err
	}
	return s, offset + int(length), nil
}

// readBytes reads a length-prefixed binary field
func readBytes(buf []byte, offset int) ([]byte, int, error) {
	length, offset, err := readUint16(buf, offset)
	if err != nil {
		return nil, offset, fmt.Errorf("truncated bytes length: %w", err)
	}
	if offset+int(length) > len(buf) {
		return nil, offset, fmt.Errorf("truncated bytes")
	}
	b := make([]byte, length)
	copy(b, buf[offset:offset+int(length)])
	return b, offset + int(length), nil
}

func validateFlags(h FixedHeader) error {
	validFlag, hasReservedFlags := reservedFlags[h.PacketType]
	if !hasReservedFlags {
		return nil // packet type is pub or is unknown, is handled by ReadPacket
	}
	if h.Flags != validFlag {
		return fmt.Errorf("invalid reserved flags 0x%X for packet type %d", h.Flags, h.PacketType)
	}
	return nil
}

func validateConnectFlags(f ConnectFlags) error {
	if !f.WillFlag {
		if f.WillQoS != 0 {
			return fmt.Errorf("will qos must be 0 when will flag is unset")
		}
		if f.WillRetain {
			return fmt.Errorf("will retain must be 0 when will flag is unset")
		}
	} else if f.WillQoS > 2 {
		return fmt.Errorf("will qos must be 0, 1 or 2, got %d", f.WillQoS)
	}

	if !f.UsernameFlag && f.PasswordFlag {
		return fmt.Errorf("password flag must be 0 when username flag is unset")
	}

	return nil
}

func validateUTF8(s string) error {
	if !utf8.ValidString(s) {
		return fmt.Errorf("ill-formed UTF-8 string")
	}
	if strings.IndexByte(s, 0x00) != -1 {
		return fmt.Errorf("UTF-8 string contains U+0000")
	}
	return nil
}

func validateTopicName(s string) error {
	if s == "" {
		return fmt.Errorf("the topic name cannot be empty")
	}
	if strings.Contains(s, "+") || strings.Contains(s, ".") {
		return fmt.Errorf("the topic name must contain wildcard characters")
	}
	return nil
}
