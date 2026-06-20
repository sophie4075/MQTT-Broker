package mqtt

type PacketType byte

const (
	CONNECT     PacketType = 1
	CONNACK     PacketType = 2
	PUBLISH     PacketType = 3
	PUBACK      PacketType = 4
	PUBREC      PacketType = 5
	PUBREL      PacketType = 6
	PUBCOMP     PacketType = 7
	SUBSCRIBE   PacketType = 8
	SUBACK      PacketType = 9
	UNSUBSCRIBE PacketType = 10
	UNSUBACK    PacketType = 11
	PINGREQ     PacketType = 12
	PINGRESP    PacketType = 13
	DISCONNECT  PacketType = 14
)

// QoS Quality-of-Service (2 Bit).
type QoS byte

const (
	AtMostOnce  QoS = 0
	AtLeastOnce QoS = 1
	ExactlyOnce QoS = 2
)

// Packet wird von allen Control-Packet-Typen implementiert.
type Packet interface {
	Type() PacketType
}

type Pingreq struct{}
type Pingresp struct{}
type Disconnect struct{}

func (p *Connect) Type() PacketType     { return CONNECT }
func (p *Connack) Type() PacketType     { return CONNACK }
func (p *Publish) Type() PacketType     { return PUBLISH }
func (p *Subscribe) Type() PacketType   { return SUBSCRIBE }
func (p *Unsubscribe) Type() PacketType { return UNSUBSCRIBE }
func (p *Suback) Type() PacketType      { return SUBACK }
func (p *Ack) Type() PacketType         { return p.Kind }
func (p *Pingreq) Type() PacketType     { return PINGREQ }
func (p *Pingresp) Type() PacketType    { return PINGRESP }
func (p *Disconnect) Type() PacketType  { return DISCONNECT }

// FixedHeader is present in each MQTT Control Packet Type
//
// Byte 1
// +--------+--------+--------+--------+--------+--------+--------+--------+
// |            PacketType           |          Flags            |
// +-------------------------------------------+---------------------------+
// |                 Bits 7..4                 |         Bits 3..0         |
// +-------------------------------------------+---------------------------+
//
// Byte 2..5
// +---------------------------------------------------------------+
// | RemainingLength (Variable Byte Integer, 1-4 bytes)           |
// +---------------------------------------------------------------+
//
// RemainingLength specifies the number of bytes following the fixed header (Variable Header + Payload).
// (See MQTT Version 3.1.1, para 2.2)
type FixedHeader struct {
	PacketType      PacketType
	Flags           byte // untere 4 Bit von Byte 1, roh (nur bei PUBLISH bedeutungstragend)
	RemainingLength int
}

// ConnectFlags byte contains a number of parameters specifying the behavior of the MQTT
// connection. It also indicates the presence or absence of fields in the payload (See MQTT Version 3.1.1, para 3.1.2.3).
// TODO: The Server MUST validate that the reserved flag in the Packet is set to zero and disconnect the Client if it is not zero
type ConnectFlags struct {
	CleanSession bool
	WillFlag     bool
	WillQoS      QoS
	WillRetain   bool
	PasswordFlag bool
	UsernameFlag bool
}

// ConnectPayload contains one or more length-prefixed fields, whose presence is
// determined by the flags in the variable header(See MQTT Version 3.1.1, para 3.1.3).
type ConnectPayload struct {
	ClientID  string
	WillTopic string
	WillMsg   []byte
	Username  string
	Password  []byte
}

// Connect Client requests a connection to a Server
type Connect struct {
	KeepAlive uint16 // Variable Header
	Flags     ConnectFlags
	Payload   ConnectPayload
}

// Connack is sent by the server in response to a CONNECT Packet received
type Connack struct {
	SessionPresent bool
	ReturnCode     byte
}

// Publish is sent from a Client to a Server or from Server to a
// Client to transport an Application Message.
type Publish struct {
	// carries flags itself, as publish only uses the flag-bits semantically
	Dup       bool
	QoS       QoS
	Retain    bool
	TopicName string
	PacketID  *uint16
	Payload   []byte
}

// TopicSubscription indicate the Topics to which the Client wants to subscribe.
type TopicSubscription struct {
	Topic string
	QoS   QoS
}

// Subscribe is sent from the Client to the Server to create one or more Subscriptions. Each
// Subscription registers a Client’s interest in one or more Topics
type Subscribe struct {
	PacketID uint16
	Topics   []TopicSubscription
}

// Unsubscribe is sent by the Client to the Server, to unsubscribe from topics.
type Unsubscribe struct {
	PacketID uint16
	Topics   []string
}

// Suback is sent by the Server to the Client to confirm receipt.
// and processing of a Subscribe.
type Suback struct {
	PacketID    uint16
	ReturnCodes []byte
}

// Ack covers packages, that only contain a packet identifier.
// Kind refers to PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK.
type Ack struct {
	Kind     PacketType
	PacketID uint16
}
