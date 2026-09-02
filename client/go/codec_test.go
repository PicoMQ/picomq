package picomq

import (
	"bytes"
	"encoding/binary"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Pico batch codec", func() {
	ginkgo.It("encodes append records deterministically", func() {
		encoded, err := encodeBatch([]AppendRecord{{Body: []byte("body"), Headers: map[string]string{"z": "last", "a": "first"}}})
		Expect(err).NotTo(HaveOccurred())
		Expect(encoded[0]).To(Equal(byte(1)))
		Expect(binary.BigEndian.Uint32(encoded[1:5])).To(Equal(uint32(1)))
		Expect(bytes.Index(encoded, []byte("a"))).To(BeNumerically("<", bytes.Index(encoded, []byte("z"))))
	})

	ginkgo.It("decodes sequenced records", func() {
		var payload bytes.Buffer
		payload.WriteByte(1)
		Expect(binary.Write(&payload, binary.BigEndian, uint32(1))).To(Succeed())
		Expect(binary.Write(&payload, binary.BigEndian, uint64(42))).To(Succeed())
		Expect(binary.Write(&payload, binary.BigEndian, int64(99))).To(Succeed())
		Expect(binary.Write(&payload, binary.BigEndian, uint32(1))).To(Succeed())
		Expect(writeBytes(&payload, []byte("kind"))).To(Succeed())
		Expect(writeBytes(&payload, []byte("test"))).To(Succeed())
		Expect(writeBytes(&payload, []byte("value"))).To(Succeed())

		records, err := decodeBatch(payload.Bytes())
		Expect(err).NotTo(HaveOccurred())
		Expect(records).To(Equal([]Record{{Position: "42", Timestamp: 99, Headers: map[string]string{"kind": "test"}, Body: []byte("value")}}))
	})

	ginkgo.It("rejects truncated payloads", func() {
		_, err := decodeBatch([]byte{1, 0, 0})
		Expect(err).To(MatchError(ContainSubstring("truncated")))
	})
})
