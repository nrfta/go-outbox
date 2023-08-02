package mb

import (
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Message struct {
	Key       []byte
	Value     []byte
	Headers   []kgo.RecordHeader
	Timestamp time.Time
	Topic     string
}
