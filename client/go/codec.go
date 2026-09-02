package picomq

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"unicode/utf8"
)

const batchVersion byte = 1

const (
	maxDecodedRecords uint32 = 4096
	maxDecodedHeaders uint32 = 1024
)

func encodeBatch(records []AppendRecord) ([]byte, error) {
	var out bytes.Buffer
	out.WriteByte(batchVersion)
	if err := binary.Write(&out, binary.BigEndian, uint32(len(records))); err != nil {
		return nil, err
	}
	for _, record := range records {
		if record.Key == nil {
			if err := binary.Write(&out, binary.BigEndian, int32(-1)); err != nil {
				return nil, err
			}
		} else {
			if uint64(len(record.Key)) > uint64(^uint32(0)>>1) {
				return nil, fmt.Errorf("record key is too large")
			}
			if err := binary.Write(&out, binary.BigEndian, int32(len(record.Key))); err != nil {
				return nil, err
			}
			if _, err := out.Write(record.Key); err != nil {
				return nil, err
			}
		}
		keys := make([]string, 0, len(record.Headers))
		for key := range record.Headers {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		if err := binary.Write(&out, binary.BigEndian, uint32(len(keys))); err != nil {
			return nil, err
		}
		for _, key := range keys {
			value := record.Headers[key]
			if !utf8.ValidString(key) || !utf8.ValidString(value) {
				return nil, fmt.Errorf("record headers must be valid UTF-8")
			}
			if err := writeBytes(&out, []byte(key)); err != nil {
				return nil, err
			}
			if err := writeBytes(&out, []byte(value)); err != nil {
				return nil, err
			}
		}
		if err := writeBytes(&out, record.Body); err != nil {
			return nil, err
		}
	}
	return out.Bytes(), nil
}

func decodeBatch(data []byte) ([]Record, error) {
	r := bytes.NewReader(data)
	version, err := r.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("truncated batch")
	}
	if version != batchVersion {
		return nil, fmt.Errorf("unknown batch version %d", version)
	}
	count, err := readU32(r)
	if err != nil {
		return nil, fmt.Errorf("truncated batch: %w", err)
	}
	records := make([]Record, 0, minU32(count, maxDecodedRecords))
	for i := uint32(0); i < count; i++ {
		seq, err := readU64(r)
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", i, err)
		}
		timestamp, err := readI64(r)
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", i, err)
		}
		key, err := readKey(r)
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", i, err)
		}
		headers, err := readHeaders(r)
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", i, err)
		}
		body, err := readBytes(r)
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", i, err)
		}
		records = append(records, Record{Position: fmt.Sprint(seq), Timestamp: timestamp, Key: key, Headers: headers, Body: body})
	}
	if r.Len() != 0 {
		return nil, fmt.Errorf("batch has %d trailing bytes", r.Len())
	}
	return records, nil
}

func writeBytes(w io.Writer, data []byte) error {
	if uint64(len(data)) > uint64(^uint32(0)) {
		return fmt.Errorf("value is too large")
	}
	if err := binary.Write(w, binary.BigEndian, uint32(len(data))); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

func readU32(r io.Reader) (uint32, error) {
	var v uint32
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}
func readU64(r io.Reader) (uint64, error) {
	var v uint64
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}
func readI64(r io.Reader) (int64, error) {
	var v int64
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}
func readI32(r io.Reader) (int32, error) {
	var v int32
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}
func readKey(r *bytes.Reader) ([]byte, error) {
	n, err := readI32(r)
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, nil
	}
	if int64(n) > int64(r.Len()) {
		return nil, io.ErrUnexpectedEOF
	}
	data := make([]byte, int(n))
	_, err = io.ReadFull(r, data)
	return data, err
}
func readBytes(r *bytes.Reader) ([]byte, error) {
	n, err := readU32(r)
	if err != nil {
		return nil, err
	}
	if uint64(n) > uint64(r.Len()) {
		return nil, io.ErrUnexpectedEOF
	}
	data := make([]byte, n)
	_, err = io.ReadFull(r, data)
	return data, err
}
func readHeaders(r *bytes.Reader) (map[string]string, error) {
	n, err := readU32(r)
	if err != nil {
		return nil, err
	}
	headers := make(map[string]string, minU32(n, maxDecodedHeaders))
	for i := uint32(0); i < n; i++ {
		key, err := readBytes(r)
		if err != nil {
			return nil, err
		}
		value, err := readBytes(r)
		if err != nil {
			return nil, err
		}
		if !utf8.Valid(key) || !utf8.Valid(value) {
			return nil, fmt.Errorf("invalid UTF-8 in headers")
		}
		headers[string(key)] = string(value)
	}
	return headers, nil
}

func minU32(value, limit uint32) uint32 {
	if value < limit {
		return value
	}
	return limit
}
