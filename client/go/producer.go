package picomq

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"
)

type ProducerConfig struct {
	Epoch            uint64
	Linger           time.Duration
	MaxBatchRecords  int
	MaxBatchBytes    int
	MaxBufferedBytes int
	Retry            RetryPolicy
}

func DefaultProducerConfig() ProducerConfig {
	return ProducerConfig{Linger: 5 * time.Millisecond, MaxBatchRecords: 500, MaxBatchBytes: 1 << 20, MaxBufferedBytes: 32 << 20, Retry: RetryPolicy{MaxAttempts: 12, InitialBackoff: time.Millisecond, MaxBackoff: 100 * time.Millisecond, Multiplier: 2}}
}

type producerItem struct {
	record AppendRecord
	size   int
	result chan pendingResult
}
type pendingResult struct {
	seq uint64
	err error
}

type Pending struct {
	result <-chan pendingResult
	once   sync.Once
	value  pendingResult
	done   chan struct{}
}

func (p *Pending) Await(ctx context.Context) (uint64, error) {
	p.once.Do(func() {
		p.done = make(chan struct{})
		go func() {
			p.value = <-p.result
			close(p.done)
		}()
	})
	select {
	case <-p.done:
		return p.value.seq, p.value.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

type Producer struct {
	client           *PicoClient
	name, id         string
	config           ProducerConfig
	queue            chan *producerItem
	budgetMu         sync.Mutex
	bufferedBytes    int
	budgetChanged    chan struct{}
	done             chan struct{}
	closeReq         chan struct{}
	closeOnce        sync.Once
	mu               sync.Mutex
	closed, poisoned bool
	err              error
}

func (s *PicoStream) NewProducer(id string, config *ProducerConfig) *Producer {
	cfg := DefaultProducerConfig()
	if config != nil {
		cfg = *config
		if cfg.MaxBatchRecords <= 0 {
			cfg.MaxBatchRecords = 500
		}
		if cfg.MaxBatchBytes <= 0 {
			cfg.MaxBatchBytes = 1 << 20
		}
		if cfg.MaxBufferedBytes <= 0 {
			cfg.MaxBufferedBytes = 32 << 20
		}
		if cfg.Retry.MaxAttempts == 0 {
			cfg.Retry = DefaultProducerConfig().Retry
		}
	}
	p := &Producer{client: s.client, name: s.name, id: id, config: cfg, queue: make(chan *producerItem), budgetChanged: make(chan struct{}), done: make(chan struct{}), closeReq: make(chan struct{})}
	go p.run()
	return p
}

func (p *Producer) Send(ctx context.Context, record AppendRecord) (*Pending, error) {
	size := len(record.Body)
	if size > p.config.MaxBufferedBytes {
		return nil, &ClientError{Kind: ErrorBadRequest, Code: "record_too_large", Message: fmt.Sprintf("record of %d bytes exceeds producer budget of %d bytes", size, p.config.MaxBufferedBytes)}
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, &ClientError{Kind: ErrorOther, Code: "producer_closed", Message: "producer is closed"}
	}
	if p.poisoned {
		err := p.err
		p.mu.Unlock()
		return nil, err
	}
	p.mu.Unlock()
	units := size
	if units < 1 {
		units = 1
	}
	if err := p.acquireBudget(ctx, units); err != nil {
		return nil, err
	}
	item := &producerItem{record: cloneRecords([]AppendRecord{record})[0], size: units, result: make(chan pendingResult, 1)}
	p.mu.Lock()
	closed, poisoned, poisonErr := p.closed, p.poisoned, p.err
	p.mu.Unlock()
	if closed {
		p.release(item)
		return nil, &ClientError{Kind: ErrorOther, Code: "producer_closed", Message: "producer is closed"}
	}
	if poisoned {
		p.release(item)
		return nil, poisonErr
	}
	select {
	case p.queue <- item:
		return &Pending{result: item.result}, nil
	case <-ctx.Done():
		p.release(item)
		return nil, ctx.Err()
	case <-p.done:
		p.release(item)
		return nil, &ClientError{Kind: ErrorOther, Code: "producer_closed", Message: "producer is closed"}
	case <-p.closeReq:
		p.release(item)
		return nil, &ClientError{Kind: ErrorOther, Code: "producer_closed", Message: "producer is closed"}
	}
}

func (p *Producer) SendDurable(ctx context.Context, record AppendRecord) (uint64, error) {
	pending, err := p.Send(ctx, record)
	if err != nil {
		return 0, err
	}
	return pending.Await(ctx)
}

func (p *Producer) Flush(ctx context.Context) error {
	for {
		p.budgetMu.Lock()
		if p.bufferedBytes == 0 {
			p.budgetMu.Unlock()
			break
		}
		changed := p.budgetChanged
		p.budgetMu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.poisoned {
		return p.err
	}
	return nil
}

func (p *Producer) Close(ctx context.Context) error {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.closeOnce.Do(func() { close(p.closeReq) })
	select {
	case <-p.done:
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Producer) run() {
	defer close(p.done)
	var seq uint64
	for {
		var first *producerItem
		select {
		case first = <-p.queue:
		case <-p.closeReq:
			return
		}
		batch := []*producerItem{first}
		bytes := len(first.record.Body)
		deadline := time.NewTimer(p.config.Linger)
	collect:
		for len(batch) < p.config.MaxBatchRecords && bytes < p.config.MaxBatchBytes {
			select {
			case item := <-p.queue:
				batch = append(batch, item)
				bytes += len(item.record.Body)
			case <-p.closeReq:
				break collect
			case <-deadline.C:
				break collect
			}
		}
		if !deadline.Stop() {
			select {
			case <-deadline.C:
			default:
			}
		}
		records := make([]AppendRecord, len(batch))
		for i, item := range batch {
			records[i] = item.record
		}
		start, err := p.sendBatch(records, seq)
		seq++
		if err != nil {
			p.fail(batch, err)
			return
		}
		for i, item := range batch {
			item.result <- pendingResult{seq: start + uint64(i)}
			p.release(item)
		}
	}
}

func (p *Producer) sendBatch(records []AppendRecord, seq uint64) (uint64, error) {
	for attempt := 0; ; attempt++ {
		ack, err := p.client.AppendAs(context.Background(), p.name, records, ProducerRef{ID: p.id, Epoch: p.config.Epoch, Seq: seq})
		if err == nil {
			position := ack.Ack.Start
			if ack.Duplicate {
				next, nerr := strconv.ParseUint(ack.Ack.Next, 10, 64)
				if nerr != nil || next < uint64(len(records)) {
					return 0, invalidResponse(fmt.Errorf("invalid duplicate next position %q", ack.Ack.Next))
				}
				return next - uint64(len(records)), nil
			}
			start, nerr := strconv.ParseUint(position, 10, 64)
			if nerr != nil {
				return 0, invalidResponse(nerr)
			}
			return start, nil
		}
		delay, again := p.config.Retry.delay(attempt)
		if !again || !retryable(err) {
			return 0, err
		}
		time.Sleep(delay)
	}
}
func (p *Producer) fail(batch []*producerItem, err error) {
	p.mu.Lock()
	p.poisoned = true
	p.err = &ClientError{Kind: ErrorConflict, Code: "producer_poisoned", Message: "producer session failed and cannot continue; create a producer with a higher epoch", Cause: err}
	poison := p.err
	p.mu.Unlock()
	for _, item := range batch {
		item.result <- pendingResult{err: poison}
		p.release(item)
	}
}
func (p *Producer) release(item *producerItem) {
	p.budgetMu.Lock()
	p.bufferedBytes -= item.size
	close(p.budgetChanged)
	p.budgetChanged = make(chan struct{})
	p.budgetMu.Unlock()
}

func (p *Producer) acquireBudget(ctx context.Context, size int) error {
	for {
		p.budgetMu.Lock()
		if p.bufferedBytes+size <= p.config.MaxBufferedBytes {
			p.bufferedBytes += size
			p.budgetMu.Unlock()
			return nil
		}
		changed := p.budgetChanged
		p.budgetMu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
