// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package rabbitmqexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/rabbitmqexporter"
import (
	"context"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/rabbitmqexporter/internal"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/rabbitmqexporter/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

type rabbitmqExporterFactory struct {
}

func NewFactory() exporter.Factory {
	f := &rabbitmqExporterFactory{}
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithTraces(f.createTracesExporter, metadata.TracesStability),
		exporter.WithMetrics(f.createMetricsExporter, metadata.MetricsStability),
		exporter.WithLogs(f.createLogsExporter, metadata.LogsStability),
	)
}

func (f *rabbitmqExporterFactory) createTracesExporter(
	ctx context.Context,
	set exporter.CreateSettings,
	cfg component.Config,
) (exporter.Traces, error) {
	customConfig := *(cfg.(*config))
	exp, err := newTracesExporter(customConfig, set, internal.NewAmqpClient())
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewTracesExporter(
		ctx,
		set,
		cfg,
		exp.exportTraces,
		exporterhelper.WithRetry(customConfig.retrySettings))
}

func (f *rabbitmqExporterFactory) createMetricsExporter(
	ctx context.Context,
	set exporter.CreateSettings,
	cfg component.Config,
) (exporter.Metrics, error) {
	customConfig := *(cfg.(*config))
	exp, err := newMetricsExporter(customConfig, set, internal.NewAmqpClient())
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewMetricsExporter(
		ctx,
		set,
		cfg,
		exp.exportMetrics,
		exporterhelper.WithRetry(customConfig.retrySettings))
}

func (f *rabbitmqExporterFactory) createLogsExporter(
	ctx context.Context,
	set exporter.CreateSettings,
	cfg component.Config,
) (exporter.Logs, error) {
	customConfig := *(cfg.(*config))
	exp, err := newLogsExporter(customConfig, set, internal.NewAmqpClient())
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewLogsExporter(
		ctx,
		set,
		cfg,
		exp.exportLogs,
		exporterhelper.WithRetry(customConfig.retrySettings))
}

func newTracesExporter(conf config, set exporter.CreateSettings, amqpClient internal.AmqpClient) (*rabbitMqTracesExporter, error) {
	publisher, err := newPublisher(conf, set, amqpClient, "otel-traces")
	if err != nil {
		return nil, err
	}

	return &rabbitMqTracesExporter{
		publisher:  publisher,
		marshaller: newTracesMarshaler(),
	}, nil
}

func newMetricsExporter(conf config, set exporter.CreateSettings, amqpClient internal.AmqpClient) (*rabbitMqMetricsExporter, error) {
	publisher, err := newPublisher(conf, set, amqpClient, "otel-metrics")
	if err != nil {
		return nil, err
	}

	return &rabbitMqMetricsExporter{
		publisher:  publisher,
		marshaller: newMetricsMarshaler(),
	}, nil
}

func newLogsExporter(conf config, set exporter.CreateSettings, amqpClient internal.AmqpClient) (*rabbitMqLogsExporter, error) {
	publisher, err := newPublisher(conf, set, amqpClient, "otel-logs")
	if err != nil {
		return nil, err
	}

	return &rabbitMqLogsExporter{
		publisher:  publisher,
		marshaller: newLogMarshaler(),
	}, nil
}

func newPublisher(conf config, set exporter.CreateSettings, client internal.AmqpClient, connectionName string) (*rabbitMqPublisher, error) {
	connectionConfig := &connectionConfig{
		logger:            set.Logger,
		connectionUrl:     conf.connectionUrl,
		connectionName:    connectionName,
		channelPoolSize:   conf.channelPoolSize,
		connectionTimeout: conf.connectionTimeout,
		heartbeatInterval: conf.connectionHeartbeatInterval,
		confirmationMode:  conf.confirmMode,
	}
	channelCacher, err := newAmqpChannelCacher(connectionConfig, client)
	if err != nil {
		return nil, err
	}

	publisher := rabbitMqPublisher{
		set:           set.TelemetrySettings,
		config:        conf,
		channelCacher: channelCacher,
	}
	return &publisher, nil
}
