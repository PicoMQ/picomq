package picomq

import (
	"bytes"
	"encoding/binary"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Pico batch codec", func() {
	ginkgo.It("encodes append records with headers in the order given", func() {
		encoded, err := encodeBatch([]AppendRecord{{Body: []byte("body"), Key: []byte("key"), Headers: []Header{{Name: "z", Value: []byte("last")}, {Name: "a", Value: []byte("first")}}}})
		Expect(err).NotTo(HaveOccurred())
		Expect(encoded[0]).To(Equal(byte(1)))
		Expect(binary.BigEndian.Uint32(encoded[1:5])).To(Equal(uint32(1)))
		Expect(int32(binary.BigEndian.Uint32(encoded[5:9]))).To(Equal(int32(3)))
		Expect(encoded[9:12]).To(Equal([]byte("key")))
		Expect(bytes.Index(encoded, []byte("last"))).To(BeNumerically("<", bytes.Index(encoded, []byte("first"))))
	})

	ginkgo.It("round trips null bodies apart from empty ones", func() {
		encoded, err := encodeBatch([]AppendRecord{{Key: []byte("gone"), Body: nil}, {Body: []byte{}}})
		Expect(err).NotTo(HaveOccurred())

		records, err := decodeAppendBatchForTest(encoded)
		Expect(err).NotTo(HaveOccurred())
		Expect(records[0].Body).To(BeNil())
		Expect(records[1].Body).NotTo(BeNil())
		Expect(records[1].Body).To(BeEmpty())
	})

	ginkgo.It("clones records without turning empty values into null", func() {
		cloned := cloneRecords([]AppendRecord{{Body: []byte{}, Headers: []Header{{Name: "e", Value: []byte{}}}}})

		Expect(cloned[0].Body).NotTo(BeNil())
		Expect(cloned[0].Headers[0].Value).NotTo(BeNil())
	})

	ginkgo.It("round trips repeated and null headers", func() {
		headers := []Header{
			{Name: "trace", Value: []byte("a")},
			{Name: "trace", Value: []byte("b")},
			{Name: "flag", Value: nil},
			{Name: "empty", Value: []byte{}},
		}
		encoded, err := encodeBatch([]AppendRecord{{Body: []byte("body"), Headers: headers}})
		Expect(err).NotTo(HaveOccurred())

		records, err := decodeAppendBatchForTest(encoded)
		Expect(err).NotTo(HaveOccurred())
		Expect(records[0].Headers).To(Equal(headers))
		Expect(records[0].Headers[2].Value).To(BeNil())
		Expect(records[0].Headers[3].Value).NotTo(BeNil())
	})

	ginkgo.It("decodes sequenced records", func() {
		var payload bytes.Buffer
		payload.WriteByte(1)
		Expect(binary.Write(&payload, binary.BigEndian, uint32(1))).To(Succeed())
		Expect(binary.Write(&payload, binary.BigEndian, uint64(42))).To(Succeed())
		Expect(binary.Write(&payload, binary.BigEndian, int64(99))).To(Succeed())
		Expect(binary.Write(&payload, binary.BigEndian, int32(3))).To(Succeed())
		payload.WriteString("key")
		Expect(binary.Write(&payload, binary.BigEndian, uint32(1))).To(Succeed())
		Expect(writeBytes(&payload, []byte("kind"))).To(Succeed())
		Expect(writeBytes(&payload, []byte{0xff, 0x00})).To(Succeed())
		Expect(writeBytes(&payload, []byte("value"))).To(Succeed())

		records, err := decodeBatch(payload.Bytes())
		Expect(err).NotTo(HaveOccurred())
		Expect(records).To(Equal([]Record{{Position: "42", Timestamp: 99, Key: []byte("key"), Headers: []Header{{Name: "kind", Value: []byte{0xff, 0x00}}}, Body: []byte("value")}}))
	})

	ginkgo.It("encodes nil keys with the no-key sentinel", func() {
		encoded, err := encodeBatch([]AppendRecord{{Body: []byte("body")}})
		Expect(err).NotTo(HaveOccurred())
		Expect(int32(binary.BigEndian.Uint32(encoded[5:9]))).To(Equal(int32(-1)))
	})

	ginkgo.It("rejects huge response-controlled counts without large allocations", func() {
		_, err := decodeBatch([]byte{1, 0xff, 0xff, 0xff, 0xff})
		Expect(err).To(HaveOccurred())

		var payload bytes.Buffer
		payload.WriteByte(1)
		Expect(binary.Write(&payload, binary.BigEndian, uint32(1))).To(Succeed())
		Expect(binary.Write(&payload, binary.BigEndian, uint64(0))).To(Succeed())
		Expect(binary.Write(&payload, binary.BigEndian, int64(0))).To(Succeed())
		Expect(binary.Write(&payload, binary.BigEndian, int32(-1))).To(Succeed())
		Expect(binary.Write(&payload, binary.BigEndian, uint32(^uint32(0)))).To(Succeed())
		_, err = decodeBatch(payload.Bytes())
		Expect(err).To(HaveOccurred())
	})

	ginkgo.It("rejects truncated payloads", func() {
		_, err := decodeBatch([]byte{1, 0, 0})
		Expect(err).To(MatchError(ContainSubstring("truncated")))
	})
})
