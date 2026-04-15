package mqtt

const (
	CONNECT     = 1
	CONNACK     = 2
	PUBLISH     = 3
	PUBACK      = 4
	PUBREC      = 5
	PUBREL      = 6
	PUBCOMP     = 7
	SUBSCRIBE   = 8
	SUBACK      = 9
	UNSUBSCRIBE = 10
	UNSUBACK    = 11
	PINGREQ     = 12
	PINGRESP    = 13
	DISCONNECT  = 14
)

const (
	AtMostOnce  = 0
	AtLeastOnce = 1
	ExactlyOnce = 2
)

type Packet interface {
	Type() byte
}

func (p *Connect) Type() byte     { return CONNECT }
func (p *Connack) Type() byte     { return CONNACK }
func (p *Publish) Type() byte     { return PUBLISH }
func (p *Subscribe) Type() byte   { return SUBSCRIBE }
func (p *Unsubscribe) Type() byte { return UNSUBSCRIBE }
func (p *Suback) Type() byte      { return SUBACK }
func (p *Ack) Type() byte         { return p.Header.PacketType }

type FixedHeader struct {
	PacketType      byte
	Dup             bool
	Qos             byte
	Retain          bool
	RemainingLength int
}

type connectPayload struct {
	KeepAlive uint16
	ClientId  string
	WillTopic string
	WillMsg   []byte
	Username  string
	Password  []byte
}

type connectFlags struct {
	Reserved     int
	CleanSession bool
	WillFlag     bool
	WillQos      byte
	WillRetain   bool
	PasswordFlag bool
	UsernameFlag bool
}

type Connect struct {
	Header  FixedHeader
	Bits    connectFlags
	Payload connectPayload
}

type Connack struct {
	Header         FixedHeader
	SessionPresent bool
	ReturnCode     byte
}

type TopicSubscription struct {
	Topic string
	Qos   byte
}
type Publish struct {
	Header    FixedHeader
	TopicName string
	PacketID  uint16
	Payload   []byte
}

type Subscribe struct {
	Header   FixedHeader
	PacketID uint16
	Topics   []TopicSubscription
}

type Unsubscribe struct {
	Header   FixedHeader
	PacketID uint16
	Topics   []string
}

type Suback struct {
	Header      FixedHeader
	PacketID    uint16
	ReturnCodes []byte
}

type Ack struct {
	Header   FixedHeader
	PacketID uint16
}

type Pingreq struct {
	Header FixedHeader
}

type Pingresp struct {
	Header FixedHeader
}

type Disconnect struct {
	Header FixedHeader
}
