package mqtt

import (
	"bytes"
	"testing"
)

func buildPacket(headerByte byte, body []byte) []byte {
	var buf bytes.Buffer
	buf.WriteByte(headerByte)
	EncodeLength(&buf, len(body))
	buf.Write(body)
	return buf.Bytes()
}

func mqttString(s string) []byte {
	b := make([]byte, 2+len(s))
	b[0] = byte(len(s) >> 8)
	b[1] = byte(len(s))
	copy(b[2:], s)
	return b
}

func TestDecodeConnect(t *testing.T) {
	var body bytes.Buffer

	body.Write(mqttString("MQTT"))
	body.WriteByte(4)
	body.WriteByte(0b11000010)
	body.Write([]byte{0x00, 0x3C})
	body.Write(mqttString("test-client"))
	body.Write(mqttString("myuser"))
	body.Write(mqttString("mypass"))

	raw := buildPacket(CONNECT<<4, body.Bytes())
	pkt, err := ReadPacket(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	conn, ok := pkt.(*Connect)
	if !ok {
		t.Fatalf("expected *Connect, got %T", pkt)
	}

	if conn.Payload.ClientId != "test-client" {
		t.Errorf("ClientId = %q, want %q", conn.Payload.ClientId, "test-client")
	}
	if conn.Payload.KeepAlive != 60 {
		t.Errorf("KeepAlive = %d, want 60", conn.Payload.KeepAlive)
	}
	if !conn.Bits.CleanSession {
		t.Error("CleanSession should be true")
	}
	if !conn.Bits.UsernameFlag {
		t.Error("UsernameFlag should be true")
	}
	if conn.Payload.Username != "myuser" {
		t.Errorf("Username = %q, want %q", conn.Payload.Username, "myuser")
	}
	if string(conn.Payload.Password) != "mypass" {
		t.Errorf("Password = %q, want %q", conn.Payload.Password, "mypass")
	}
	if conn.Bits.WillFlag {
		t.Error("WillFlag should be false")
	}
}

func TestDecodeConnectWithWill(t *testing.T) {
	var body bytes.Buffer

	body.Write(mqttString("MQTT"))
	body.WriteByte(4)
	body.WriteByte(0b00001110)
	body.Write([]byte{0x00, 0x1E}) // 30

	body.Write(mqttString("client-will"))
	body.Write(mqttString("will/topic"))
	body.Write(mqttString("goodbye"))

	raw := buildPacket(CONNECT<<4, body.Bytes())
	pkt, err := ReadPacket(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	conn := pkt.(*Connect)
	if !conn.Bits.WillFlag {
		t.Error("WillFlag should be true")
	}
	if conn.Bits.WillQos != 1 {
		t.Errorf("WillQos = %d, want 1", conn.Bits.WillQos)
	}
	if conn.Payload.WillTopic != "will/topic" {
		t.Errorf("WillTopic = %q, want %q", conn.Payload.WillTopic, "will/topic")
	}
	if string(conn.Payload.WillMsg) != "goodbye" {
		t.Errorf("WillMsg = %q, want %q", conn.Payload.WillMsg, "goodbye")
	}
}

func TestDecodePublishQoS0(t *testing.T) {
	var body bytes.Buffer

	body.Write(mqttString("sensor/temp"))
	body.Write([]byte("22.5"))

	headerByte := byte(PUBLISH << 4)
	raw := buildPacket(headerByte, body.Bytes())

	pkt, err := ReadPacket(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	pub := pkt.(*Publish)
	if pub.TopicName != "sensor/temp" {
		t.Errorf("Topic = %q, want %q", pub.TopicName, "sensor/temp")
	}
	if string(pub.Payload) != "22.5" {
		t.Errorf("Payload = %q, want %q", pub.Payload, "22.5")
	}
	if pub.PacketID != 0 {
		t.Errorf("PacketID = %d, want 0 for QoS 0", pub.PacketID)
	}
}

func TestDecodePublishQoS1(t *testing.T) {
	var body bytes.Buffer

	body.Write(mqttString("home/light"))
	body.Write([]byte{0x00, 0x0A})
	body.Write([]byte("on"))

	headerByte := byte(PUBLISH<<4) | 0x02
	raw := buildPacket(headerByte, body.Bytes())

	pkt, err := ReadPacket(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	pub := pkt.(*Publish)
	if pub.PacketID != 10 {
		t.Errorf("PacketID = %d, want 10", pub.PacketID)
	}
	if pub.TopicName != "home/light" {
		t.Errorf("Topic = %q, want %q", pub.TopicName, "home/light")
	}
	if string(pub.Payload) != "on" {
		t.Errorf("Payload = %q, want %q", pub.Payload, "on")
	}
}

func TestDecodeSubscribeSingleTopic(t *testing.T) {
	var body bytes.Buffer

	body.Write([]byte{0x00, 0x01})
	body.Write(mqttString("home/temp"))
	body.WriteByte(1)

	headerByte := byte(SUBSCRIBE<<4) | 0x02
	raw := buildPacket(headerByte, body.Bytes())

	pkt, err := ReadPacket(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sub := pkt.(*Subscribe)
	if sub.PacketID != 1 {
		t.Errorf("PacketID = %d, want 1", sub.PacketID)
	}
	if len(sub.Topics) != 1 {
		t.Fatalf("got %d topics, want 1", len(sub.Topics))
	}
	if sub.Topics[0].Topic != "home/temp" {
		t.Errorf("Topic = %q, want %q", sub.Topics[0].Topic, "home/temp")
	}
	if sub.Topics[0].Qos != 1 {
		t.Errorf("Qos = %d, want 1", sub.Topics[0].Qos)
	}
}

func TestDecodeSubscribeMultipleTopics(t *testing.T) {
	var body bytes.Buffer

	body.Write([]byte{0x00, 0x05})
	body.Write(mqttString("a/b"))
	body.WriteByte(0)
	body.Write(mqttString("c/d"))
	body.WriteByte(2)
	body.Write(mqttString("e/f"))
	body.WriteByte(1)

	headerByte := byte(SUBSCRIBE<<4) | 0x02
	raw := buildPacket(headerByte, body.Bytes())

	pkt, err := ReadPacket(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sub := pkt.(*Subscribe)
	if len(sub.Topics) != 3 {
		t.Fatalf("got %d topics, want 3", len(sub.Topics))
	}

	expected := []struct {
		topic string
		qos   byte
	}{
		{"a/b", 0},
		{"c/d", 2},
		{"e/f", 1},
	}
	for i, e := range expected {
		if sub.Topics[i].Topic != e.topic {
			t.Errorf("topic[%d] = %q, want %q", i, sub.Topics[i].Topic, e.topic)
		}
		if sub.Topics[i].Qos != e.qos {
			t.Errorf("qos[%d] = %d, want %d", i, sub.Topics[i].Qos, e.qos)
		}
	}
}

func TestReadPacketUnsubscribeSingle(t *testing.T) {
	var body bytes.Buffer

	body.Write([]byte{0x00, 0x05})
	body.Write([]byte{0x00, 0x03, 'a', '/', 'b'})

	raw := buildPacket(0xA2, body.Bytes())
	pkt, err := ReadPacket(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	unsub, ok := pkt.(*Unsubscribe)
	if !ok {
		t.Fatalf("expected *Unsubscribe, got %T", pkt)
	}
	if unsub.PacketID != 5 {
		t.Errorf("PacketID = %d, want 5", unsub.PacketID)
	}
	if len(unsub.Topics) != 1 || unsub.Topics[0] != "a/b" {
		t.Errorf("Topics = %v, want [a/b]", unsub.Topics)
	}
}

func TestReadPacketUnsubscribeMultiple(t *testing.T) {
	var body bytes.Buffer

	body.Write([]byte{0x00, 0x03})
	body.Write([]byte{0x00, 0x05, 'h', 'e', 'l', 'l', 'o'})
	body.Write([]byte{0x00, 0x05, 'w', 'o', 'r', 'l', 'd'})

	raw := buildPacket(0xA2, body.Bytes())
	pkt, err := ReadPacket(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	unsub := pkt.(*Unsubscribe)
	if len(unsub.Topics) != 2 {
		t.Fatalf("got %d topics, want 2", len(unsub.Topics))
	}
	if unsub.Topics[0] != "hello" || unsub.Topics[1] != "world" {
		t.Errorf("Topics = %v, want [hello world]", unsub.Topics)
	}
}

func TestReadPacketAckTypes(t *testing.T) {
	tests := []struct {
		name       string
		headerByte byte
		wantType   byte
	}{
		{"PUBACK", 0x40, PUBACK},
		{"PUBREC", 0x50, PUBREC},
		{"PUBREL", 0x62, PUBREL},
		{"PUBCOMP", 0x70, PUBCOMP},
		{"UNSUBACK", 0xB0, UNSUBACK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := []byte{0x00, 0x0A}

			raw := buildPacket(tt.headerByte, body)
			pkt, err := ReadPacket(bytes.NewReader(raw))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			ack, ok := pkt.(*Ack)
			if !ok {
				t.Fatalf("expected *Ack, got %T", pkt)
			}
			if ack.Type() != tt.wantType {
				t.Errorf("Type() = %d, want %d", ack.Type(), tt.wantType)
			}
			if ack.PacketID != 10 {
				t.Errorf("PacketID = %d, want 10", ack.PacketID)
			}
		})
	}
}

func TestReadPacketHeaderOnly(t *testing.T) {
	tests := []struct {
		name       string
		headerByte byte
		wantType   byte
	}{
		{"PINGREQ", 0xC0, PINGREQ},
		{"PINGRESP", 0xD0, PINGRESP},
		{"DISCONNECT", 0xE0, DISCONNECT},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := buildPacket(tt.headerByte, []byte{})
			pkt, err := ReadPacket(bytes.NewReader(raw))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			ack, ok := pkt.(*Ack)
			if !ok {
				t.Fatalf("expected *Ack, got %T", pkt)
			}
			if ack.Type() != tt.wantType {
				t.Errorf("Type() = %d, want %d", ack.Type(), tt.wantType)
			}
			if ack.PacketID != 0 {
				t.Errorf("PacketID = %d, want 0", ack.PacketID)
			}
		})
	}
}
