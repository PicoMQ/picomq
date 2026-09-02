package picomq

import "time"

type Protocol string

const (
	ProtocolPico Protocol = "pico"
	ProtocolDS   Protocol = "ds"
)

type LiveMode uint8

const (
	LiveOff LiveMode = iota
	LiveLongPoll
)

type StreamInfo struct {
	Name        string
	ContentType string
	Start       string
	Next        string
	Closed      bool
	TTL         time.Duration
	ExpiresAt   string
}

type AppendRecord struct {
	Body        []byte
	Headers     map[string]string
	Timestamp   int64
	ContentType string
}

type Record struct {
	Position  string
	Timestamp int64
	Headers   map[string]string
	Body      []byte
}

type AppendAck struct {
	Start     string
	Next      string
	Timestamp int64
}

type ProducerRef struct {
	ID    string
	Epoch uint64
	Seq   uint64
}

type ProducerAck struct {
	Applied   bool
	Duplicate bool
	Ack       AppendAck
}

type ReadLimits struct {
	Count uint64
	Bytes uint64
}

type ReadOptions struct {
	Live   LiveMode
	Limits ReadLimits
}

type ReadPage struct {
	Records  []Record
	Next     string
	UpToDate bool
	Closed   bool
}

type StreamListing struct {
	Streams []StreamInfo
	HasMore bool
}

type RecordsOptions struct {
	From   string
	Live   bool
	Limits ReadLimits
}

type EventType string

const (
	EventData    EventType = "data"
	EventControl EventType = "control"
)

type Event struct {
	Type     EventType
	ID       string
	Records  []Record
	Raw      []byte
	Next     string
	UpToDate bool
	Closed   bool
}

type SubscribeOptions struct {
	DisableReconnect     bool
	MaxReconnectAttempts int
	ReconnectDelay       time.Duration
	MaxReconnectDelay    time.Duration
	MaxEventBytes        int
}
