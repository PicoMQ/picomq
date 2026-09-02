package picomq

import (
	"context"
	"io"
	"time"
)

type streamAPI interface {
	Create(context.Context, string, string, time.Duration) (bool, error)
	Head(context.Context, string) (*StreamInfo, error)
	Append(context.Context, string, []AppendRecord) (AppendAck, error)
	Read(context.Context, string, string, ReadOptions) (ReadPage, error)
	Close(context.Context, string) (string, error)
	Delete(context.Context, string) (bool, error)
}

type Stream struct {
	api       streamAPI
	name      string
	beginning string
}

type PicoStream struct {
	client *PicoClient
	name   string
}

func (s *Stream) Name() string     { return s.name }
func (s *PicoStream) Name() string { return s.name }
func (s *Stream) Create(ctx context.Context, contentType string, ttl time.Duration) (bool, error) {
	return s.api.Create(ctx, s.name, contentType, ttl)
}
func (s *PicoStream) Create(ctx context.Context, contentType string, ttl time.Duration) (bool, error) {
	return s.client.Create(ctx, s.name, contentType, ttl)
}
func (s *Stream) Head(ctx context.Context) (*StreamInfo, error) { return s.api.Head(ctx, s.name) }
func (s *PicoStream) Head(ctx context.Context) (*StreamInfo, error) {
	return s.client.Head(ctx, s.name)
}
func (s *Stream) Append(ctx context.Context, records ...AppendRecord) (AppendAck, error) {
	return s.api.Append(ctx, s.name, records)
}
func (s *PicoStream) Append(ctx context.Context, records ...AppendRecord) (AppendAck, error) {
	return s.client.Append(ctx, s.name, records)
}
func (s *Stream) Read(ctx context.Context, from string, options ReadOptions) (ReadPage, error) {
	return s.api.Read(ctx, s.name, from, options)
}
func (s *PicoStream) Read(ctx context.Context, from string, options ReadOptions) (ReadPage, error) {
	return s.client.Read(ctx, s.name, from, options)
}
func (s *Stream) Close(ctx context.Context) (string, error)     { return s.api.Close(ctx, s.name) }
func (s *PicoStream) Close(ctx context.Context) (string, error) { return s.client.Close(ctx, s.name) }
func (s *Stream) Delete(ctx context.Context) (bool, error)      { return s.api.Delete(ctx, s.name) }
func (s *PicoStream) Delete(ctx context.Context) (bool, error)  { return s.client.Delete(ctx, s.name) }
func (s *PicoStream) Trim(ctx context.Context, seq uint64) (string, error) {
	return s.client.Trim(ctx, s.name, seq)
}
func (s *PicoStream) AppendAs(ctx context.Context, records []AppendRecord, producer ProducerRef) (ProducerAck, error) {
	return s.client.AppendAs(ctx, s.name, records, producer)
}
func (s *Stream) Subscribe(ctx context.Context, from string, options SubscribeOptions) *Subscription {
	switch client := s.api.(type) {
	case *DurableStreamsClient:
		return client.Subscribe(ctx, s.name, from, options)
	case *PicoClient:
		return client.Subscribe(ctx, s.name, from, options)
	default:
		panic("picomq: unsupported stream client")
	}
}
func (s *PicoStream) Subscribe(ctx context.Context, from string, options SubscribeOptions) *Subscription {
	return s.client.Subscribe(ctx, s.name, from, options)
}

func (s *Stream) Records(options RecordsOptions) *RecordIterator {
	from := options.From
	if from == "" {
		from = s.beginning
	}
	return &RecordIterator{ctxAPI: s.api, name: s.name, next: from, live: options.Live, limits: options.Limits}
}
func (s *PicoStream) Records(options RecordsOptions) *RecordIterator {
	from := options.From
	if from == "" {
		from = s.client.Beginning()
	}
	return &RecordIterator{ctxAPI: s.client, name: s.name, next: from, live: options.Live, limits: options.Limits}
}

type RecordIterator struct {
	ctxAPI streamAPI
	name   string
	next   string
	live   bool
	limits ReadLimits
	page   []Record
	index  int
	done   bool
}

func (it *RecordIterator) Next(ctx context.Context) (Record, error) {
	for {
		if it.index < len(it.page) {
			record := it.page[it.index]
			it.index++
			return record, nil
		}
		if it.done {
			return Record{}, io.EOF
		}
		before := it.next
		mode := LiveOff
		if it.live {
			mode = LiveLongPoll
		}
		page, err := it.ctxAPI.Read(ctx, it.name, it.next, ReadOptions{Live: mode, Limits: it.limits})
		if err != nil {
			return Record{}, err
		}
		it.next = page.Next
		it.page = page.Records
		it.index = 0
		if page.Closed || (!it.live && page.UpToDate) || (!it.live && len(page.Records) == 0 && page.Next == before) {
			it.done = true
		}
	}
}

func (it *RecordIterator) Position() string { return it.next }
