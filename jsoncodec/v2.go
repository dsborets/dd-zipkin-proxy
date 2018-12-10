package jsoncodec

import (
	"strconv"

	"github.com/flachnetz/dd-zipkin-proxy/cache"
	"github.com/openzipkin/zipkin-go-opentracing/thrift/gen-go/zipkincore"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var log = logrus.WithField("prefix", "jsoncodec")

type SpanV2 struct {
	TraceID  Id  `json:"traceId"`
	ID       Id  `json:"id"`
	ParentID *Id `json:"parentId"`

	Name string `json:"name"`

	Endpoint *Endpoint `json:"localEndpoint"`

	Tags map[string]string `json:"tags"`

	Kind      string      `json:"kind"`
	Timestamp interface{} `json:"timestamp"`
	Duration  interface{} `json:"duration"`
}

func (span *SpanV2) ToZipkincoreSpan() *zipkincore.Span {
	var annotations []*zipkincore.Annotation

	endpoint := endpointToZipkin(span.Endpoint)

	if len(span.Tags) == 0 {
		span.Tags = map[string]string{}
		span.Tags["dd.name"] = span.Name
	}

	var binaryAnnotations []*zipkincore.BinaryAnnotation
	for key, value := range span.Tags {
		binaryAnnotations = append(binaryAnnotations, &zipkincore.BinaryAnnotation{
			Key:            cache.String(key),
			Value:          toBytesCached(value),
			Host:           endpoint,
			AnnotationType: zipkincore.AnnotationType_STRING,
		})
	}

	// in root spans the traceId equals the span id.
	parentId := span.ParentID
	if span.TraceID == span.ID {
		parentId = nil
	}

	var timeStamp int64
	var duration int64
	var err error

	switch span.Timestamp.(type) {
	case string:
		timeStamp, err = strconv.ParseInt(span.Timestamp.(string), 10, 64)
	case float64:
		timeStamp = int64(span.Timestamp.(float64))
	default:
		err = errors.New("Incorrect data type for `Timestamp`")
	}

	switch span.Duration.(type) {
	case string:
		duration, err = strconv.ParseInt(span.Duration.(string), 10, 64)
	case float64:
		duration = int64(span.Duration.(float64))
	default:
		err = errors.New("Incorrect data type for `Duration`")
	}

	if err != nil {
		log.Warn(err)
		return nil
	}

	times := [2]int64{timeStamp, duration}

	return &zipkincore.Span{
		TraceID: int64(span.TraceID),
		ID:      int64(span.ID),
		Name:    cache.String(span.Name),

		ParentID: (*int64)(parentId),

		Annotations:       annotations,
		BinaryAnnotations: binaryAnnotations,

		Timestamp: &times[0],
		Duration:  &times[1],
	}
}
