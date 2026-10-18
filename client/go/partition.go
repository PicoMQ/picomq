package picomq

import "context"

type Partition struct {
	client *DurableStreamsClient
	name   string
	index  uint32
}

type PicoPartition struct {
	client *PicoClient
	name   string
	index  uint32
}

func (p *Partition) Name() string { return p.name }

func (p *PicoPartition) Name() string { return p.name }

func (p *Partition) Head(ctx context.Context) (*StreamInfo, error) {
	return p.client.head(ctx, p.name, &p.index)
}

func (p *PicoPartition) Head(ctx context.Context) (*StreamInfo, error) {
	return p.client.head(ctx, p.name, &p.index)
}

func (p *Partition) Append(ctx context.Context, records ...AppendRecord) (AppendAck, error) {
	return p.client.append(ctx, p.name, &p.index, records)
}

func (p *PicoPartition) Append(ctx context.Context, records ...AppendRecord) (AppendAck, error) {
	return p.client.append(ctx, p.name, &p.index, records)
}

func (p *PicoPartition) AppendAs(ctx context.Context, records []AppendRecord, producer ProducerRef) (ProducerAck, error) {
	return p.client.appendAs(ctx, p.name, &p.index, records, producer)
}

func (p *Partition) Read(ctx context.Context, from string, options ReadOptions) (ReadPage, error) {
	return p.client.read(ctx, p.name, &p.index, from, options)
}

func (p *PicoPartition) Read(ctx context.Context, from string, options ReadOptions) (ReadPage, error) {
	return p.client.read(ctx, p.name, &p.index, from, options)
}

func (p *PicoPartition) Trim(ctx context.Context, seq uint64) (string, error) {
	return p.client.trim(ctx, p.name, &p.index, seq)
}

func (p *Partition) Subscribe(ctx context.Context, from string, options SubscribeOptions) *Subscription {
	return p.client.subscribe(ctx, p.name, &p.index, from, options)
}

func (p *PicoPartition) Subscribe(ctx context.Context, from string, options SubscribeOptions) *Subscription {
	return p.client.subscribe(ctx, p.name, &p.index, from, options)
}

func (p *Partition) Records(options RecordsOptions) *RecordIterator {
	return newRecordIterator(p.Read, p.client.Beginning(), options)
}

func (p *PicoPartition) Records(options RecordsOptions) *RecordIterator {
	return newRecordIterator(p.Read, p.client.Beginning(), options)
}
