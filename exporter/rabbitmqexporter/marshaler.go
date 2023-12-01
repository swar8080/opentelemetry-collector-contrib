package rabbitmqexporter

import (
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type publishingData struct {
	ContentType     string
	ContentEncoding string
	Body            []byte
}

type TracesMarshaler interface {
	Marshal(traces ptrace.Traces) (*publishingData, error)
}

type MetricsMarshaler interface {
	Marshal(metrics pmetric.Metrics) (*publishingData, error)
}

type LogsMarshaler interface {
	Marshal(logs plog.Logs) (*publishingData, error)
}

type defaultTracesMarshaler struct {
	impl *ptrace.JSONMarshaler
}

func newTracesMarshaler() TracesMarshaler {
	// TODO revisit which encoding(s) to use
	return &defaultTracesMarshaler{
		impl: &ptrace.JSONMarshaler{},
	}
}

func (m *defaultTracesMarshaler) Marshal(traces ptrace.Traces) (*publishingData, error) {
	body, err := m.impl.MarshalTraces(traces)
	if err != nil {
		return nil, err
	}

	return &publishingData{
		ContentType:     "text/plain",
		ContentEncoding: "",
		Body:            body,
	}, nil
}

type defaultMetricsMarshaler struct {
	impl *pmetric.JSONMarshaler
}

func newMetricsMarshaler() MetricsMarshaler {
	// TODO revisit which encoding(s) to use
	return &defaultMetricsMarshaler{
		impl: &pmetric.JSONMarshaler{},
	}
}

func (m *defaultMetricsMarshaler) Marshal(Metrics pmetric.Metrics) (*publishingData, error) {
	body, err := m.impl.MarshalMetrics(Metrics)
	if err != nil {
		return nil, err
	}

	return &publishingData{
		ContentType:     "text/plain",
		ContentEncoding: "",
		Body:            body,
	}, nil
}

type defaultLogsMarshaler struct {
	impl *plog.JSONMarshaler
}

func newLogMarshaler() LogsMarshaler {
	// TODO revisit which encoding(s) to use
	return &defaultLogsMarshaler{
		impl: &plog.JSONMarshaler{},
	}
}

func (m *defaultLogsMarshaler) Marshal(logs plog.Logs) (*publishingData, error) {
	body, err := m.impl.MarshalLogs(logs)
	if err != nil {
		return nil, err
	}

	return &publishingData{
		ContentType:     "text/plain",
		ContentEncoding: "",
		Body:            body,
	}, nil
}
