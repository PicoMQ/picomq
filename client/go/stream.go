package picomq

import (
	"context"
	"io"
)

type Stream struct {
	client *DurableStreamsClient
	name   string
}

type PicoStream struct {
	client *PicoClient
	name   string
}

func (s *Stream) Name() string { return s.name }

func (s *PicoStream) Name() string { return s.name }

func (s *Stream) Partition(index uint32) *Partition {
	return &Partition{client: s.client, name: s.name, index: index}
}

func (s *PicoStream) Partition(index uint32) *PicoPartition {
	return &PicoPartition{client: s.client, name: s.name, index: index}
}

func (s *Stream) Create(ctx context.Context, options CreateOptions) (bool, error) {
	return s.client.Create(ctx, s.name, options)
}

func (s *PicoStream) Create(ctx context.Context, options CreateOptions) (bool, error) {
	return s.client.Create(ctx, s.name, options)
}

func (s *Stream) Head(ctx context.Context) (*StreamInfo, error) {
	return s.client.head(ctx, s.name, nil)
}

func (s *PicoStream) Head(ctx context.Context) (*StreamInfo, error) {
	return s.client.head(ctx, s.name, nil)
}

func (s *Stream) Append(ctx context.Context, records ...AppendRecord) (AppendAck, error) {
	return s.client.append(ctx, s.name, nil, records)
}

func (s *PicoStream) Append(ctx context.Context, records ...AppendRecord) (AppendAck, error) {
	return s.client.append(ctx, s.name, nil, records)
}

func (s *PicoStream) AppendAs(ctx context.Context, records []AppendRecord, producer ProducerRef) (ProducerAck, error) {
	return s.client.appendAs(ctx, s.name, nil, records, producer)
}

func (s *Stream) Read(ctx context.Context, from string, options ReadOptions) (ReadPage, error) {
	return s.client.read(ctx, s.name, nil, from, options)
}

func (s *PicoStream) Read(ctx context.Context, from string, options ReadOptions) (ReadPage, error) {
	return s.client.read(ctx, s.name, nil, from, options)
}

func (s *PicoStream) Trim(ctx context.Context, seq uint64) (string, error) {
	return s.client.trim(ctx, s.name, nil, seq)
}

func (s *Stream) Subscribe(ctx context.Context, from string, options SubscribeOptions) *Subscription {
	return s.client.subscribe(ctx, s.name, nil, from, options)
}

func (s *PicoStream) Subscribe(ctx context.Context, from string, options SubscribeOptions) *Subscription {
	return s.client.subscribe(ctx, s.name, nil, from, options)
}

func (s *Stream) Records(options RecordsOptions) *RecordIterator {
	return newRecordIterator(s.Read, s.client.Beginning(), options)
}

func (s *PicoStream) Records(options RecordsOptions) *RecordIterator {
	return newRecordIterator(s.Read, s.client.Beginning(), options)
}

func (s *Stream) Close(ctx context.Context) (string, error) { return s.client.Close(ctx, s.name) }

func (s *PicoStream) Close(ctx context.Context) (string, error) { return s.client.Close(ctx, s.name) }

func (s *Stream) Delete(ctx context.Context) (bool, error) { return s.client.Delete(ctx, s.name) }

func (s *PicoStream) Delete(ctx context.Context) (bool, error) { return s.client.Delete(ctx, s.name) }

type RecordIterator struct {
	read   func(context.Context, string, ReadOptions) (ReadPage, error)
	next   string
	live   bool
	limits ReadLimits
	page   []Record
	index  int
	done   bool
}

func newRecordIterator(read func(context.Context, string, ReadOptions) (ReadPage, error), beginning string, options RecordsOptions) *RecordIterator {
	from := options.From
	if from == "" {
		from = beginning
	}

	return &RecordIterator{read: read, next: from, live: options.Live, limits: options.Limits}
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

		page, err := it.read(ctx, it.next, ReadOptions{Live: mode, Limits: it.limits})
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
