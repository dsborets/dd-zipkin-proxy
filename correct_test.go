package zipkinproxy

import (
	"encoding/json"
	"math/rand"
	"strings"
	"testing"

	"github.com/flachnetz/dd-zipkin-proxy/jsoncodec"
	. "github.com/onsi/gomega"
	"github.com/openzipkin/zipkin-go-opentracing/thrift/gen-go/zipkincore"
)

func TestTree(t *testing.T) {
	RegisterTestingT(t)

	firstSpan := &zipkincore.Span{ID: 1}
	secondSpan := &zipkincore.Span{ID: 2, ParentID: &firstSpan.ID}

	tree := newTree()
	tree.AddSpan(firstSpan)
	tree.AddSpan(secondSpan)

	Expect(tree.ChildrenOf(firstSpan)).To(HaveLen(1))
	Expect(tree.ChildrenOf(firstSpan)[0]).To(Equal(secondSpan))

	Expect(tree.Root()).To(Equal(firstSpan))
}

func TestMergeSpansInPlace_Annotations(t *testing.T) {
	RegisterTestingT(t)

	firstSpan := &zipkincore.Span{}
	firstSpan.Annotations = []*zipkincore.Annotation{{Value: "first"}}

	secondSpan := &zipkincore.Span{}
	secondSpan.Annotations = []*zipkincore.Annotation{{Value: "second"}}

	mergeSpansInPlace(firstSpan, secondSpan)

	Expect(firstSpan.Annotations).To(HaveLen(2))
	Expect(firstSpan.Annotations[0].Value).To(Equal("first"))
	Expect(firstSpan.Annotations[1].Value).To(Equal("second"))
}

func TestMergeSpansInPlace_Annotations_Reverse(t *testing.T) {
	RegisterTestingT(t)

	firstSpan := &zipkincore.Span{}
	firstSpan.Annotations = []*zipkincore.Annotation{{Value: "first"}}

	secondSpan := &zipkincore.Span{}
	secondSpan.Annotations = []*zipkincore.Annotation{{Value: "second"}, {Value: "sr"}}

	mergeSpansInPlace(firstSpan, secondSpan)

	Expect(firstSpan.Annotations).To(HaveLen(3))
	Expect(firstSpan.Annotations[0].Value).To(Equal("second"))
	Expect(firstSpan.Annotations[2].Value).To(Equal("first"))
}

func TestMergeSpansInPlace_BinaryAnnotations(t *testing.T) {
	RegisterTestingT(t)

	// this is the server span
	firstSpan := &zipkincore.Span{}
	firstSpan.Annotations = []*zipkincore.Annotation{{Value: "sr"}}
	firstSpan.BinaryAnnotations = []*zipkincore.BinaryAnnotation{{Key: "first"}}

	secondSpan := &zipkincore.Span{}
	secondSpan.BinaryAnnotations = []*zipkincore.BinaryAnnotation{{Key: "second"}}

	mergeSpansInPlace(firstSpan, secondSpan)

	Expect(firstSpan.BinaryAnnotations).To(HaveLen(2))
	Expect(firstSpan.BinaryAnnotations[0].Key).To(Equal("first"))
	Expect(firstSpan.BinaryAnnotations[1].Key).To(Equal("second"))
}

func TestMergeSpansInPlace_BinaryAnnotations_Reverse(t *testing.T) {
	RegisterTestingT(t)

	firstSpan := &zipkincore.Span{}
	firstSpan.BinaryAnnotations = []*zipkincore.BinaryAnnotation{{Key: "first"}}

	// this is the server span
	secondSpan := &zipkincore.Span{}
	secondSpan.Annotations = []*zipkincore.Annotation{{Value: "sr"}}
	secondSpan.BinaryAnnotations = []*zipkincore.BinaryAnnotation{{Key: "second"}}

	mergeSpansInPlace(firstSpan, secondSpan)

	Expect(firstSpan.BinaryAnnotations).To(HaveLen(2))
	Expect(firstSpan.BinaryAnnotations[0].Key).To(Equal("second"))
	Expect(firstSpan.BinaryAnnotations[1].Key).To(Equal("first"))
}

func TestCorrectTimings(t *testing.T) {
	RegisterTestingT(t)

	for i := 0; i < 100; i++ {
		indices := rand.Perm(4)
		baseOffset := rand.Int31n(10000)

		client, sharedClient, sharedServer, server := threeSpans(100, 200, 1110, 1190)

		tree := newTree()

		// add spans in random order to the tree.
		spans := []*zipkincore.Span{client, sharedClient, sharedServer, server}
		for idx, _ := range spans {
			tree.AddSpan(spans[indices[idx]])
		}

		correctTreeTimings(tree, client, int64(baseOffset))

		Expect(*client.Timestamp).To(BeEquivalentTo(baseOffset + 100))
		Expect(*server.Timestamp).To(BeEquivalentTo(baseOffset + 110))

		shared := tree.GetSpan(*sharedClient.ParentID, sharedClient.ID)
		Expect(*shared.Timestamp).To(BeEquivalentTo(baseOffset + 100))
	}
}

func TestSpanV2TimigAndDurationAsString(t *testing.T) {
	RegisterTestingT(t)
	span := &jsoncodec.SpanV2{ID: 1, Timestamp: "1234567890123456", Duration: "1234567890123457"}
	Expect(*span.ToZipkincoreSpan().Duration).To(BeEquivalentTo(int64(1234567890123457)))
	Expect(*span.ToZipkincoreSpan().Timestamp).To(BeEquivalentTo(int64(1234567890123456)))

	var parsedSpansV2 []jsoncodec.SpanV2
	var body = "[{\"traceId\":\"076290d0ed624165b71daae0b7d38354\",\"name\":\"/\",\"id\":\"4c9cb86a4bee4905\",\"parentId\":null,\"kind\":\"SERVER\",\"timestamp\":\"1544326139547000\",\"duration\":\"1456809345693245\",\"debug\":true,\"shared\":true,\"localEndpoint\":{\"serviceName\":\"some-service-name\"}}]"
	json.NewDecoder(strings.NewReader(body)).Decode(&parsedSpansV2)

	spanFomJSON := parsedSpansV2[0]
	Expect(*spanFomJSON.ToZipkincoreSpan().Duration).To(BeEquivalentTo(int64(1456809345693245)))
	Expect(*spanFomJSON.ToZipkincoreSpan().Timestamp).To(BeEquivalentTo(int64(1544326139547000)))
}

func TestSpanV2TimigAndDurationAsNumber(t *testing.T) {
	RegisterTestingT(t)

	var parsedSpansV2 []jsoncodec.SpanV2
	var body = "[{\"traceId\":\"076290d0ed624165b71daae0b7d38354\",\"name\":\"/\",\"id\":\"4c9cb86a4bee4905\",\"parentId\":null,\"kind\":\"SERVER\",\"timestamp\":1544326139547000,\"duration\":1456809,\"debug\":true,\"shared\":true,\"localEndpoint\":{\"serviceName\":\"some-service-name\"}}]"
	json.NewDecoder(strings.NewReader(body)).Decode(&parsedSpansV2)

	spanFomJSON := parsedSpansV2[0]
	Expect(*spanFomJSON.ToZipkincoreSpan().Duration).To(BeEquivalentTo(int64(1456809)))
	Expect(*spanFomJSON.ToZipkincoreSpan().Timestamp).To(BeEquivalentTo(int64(1544326139547000)))
}

func newInt64(i int64) *int64 {
	return &i
}

func threeSpans(cs, cr, sr, ss int64) (*zipkincore.Span, *zipkincore.Span, *zipkincore.Span, *zipkincore.Span) {
	client := &zipkincore.Span{ID: 1, Timestamp: newInt64(cs), Duration: newInt64(cr - cs)}
	client.Annotations = []*zipkincore.Annotation{{Value: "cs", Timestamp: cs}, {Value: "cr", Timestamp: cr}}

	sharedClient := &zipkincore.Span{ID: 2, ParentID: &client.ID, Timestamp: newInt64(cs), Duration: newInt64(cr - cs)}
	sharedClient.Annotations = []*zipkincore.Annotation{{Value: "cs", Timestamp: cs}, {Value: "cr", Timestamp: cr}}

	sharedServer := &zipkincore.Span{ID: 2, ParentID: &client.ID, Timestamp: newInt64(sr), Duration: newInt64(ss - sr)}
	sharedServer.Annotations = []*zipkincore.Annotation{{Value: "sr", Timestamp: sr}, {Value: "ss", Timestamp: ss}}

	server := &zipkincore.Span{ID: 3, ParentID: &sharedServer.ID, Timestamp: newInt64(sr), Duration: newInt64(ss - sr)}
	server.Annotations = []*zipkincore.Annotation{{Value: "sr", Timestamp: sr}, {Value: "ss", Timestamp: ss}}

	return client, sharedClient, sharedServer, server
}
