package rabbitmqexporter

import (
	"context"
	"errors"
	"fmt"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/rabbitmqexporter/internal"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"time"
)

type rabbitMqPublisher struct {
	set           component.TelemetrySettings
	config        config
	amqpClient    *internal.AmqpClient
	channelCacher *amqpChannelCacher
}

type rabbitMqTracesExporter struct {
	publisher  *rabbitMqPublisher
	marshaller TracesMarshaler
}

type rabbitMqMetricsExporter struct {
	publisher  *rabbitMqPublisher
	marshaller MetricsMarshaler
}

type rabbitMqLogsExporter struct {
	publisher  *rabbitMqPublisher
	marshaller LogsMarshaler
}

func (e *rabbitMqTracesExporter) exportTraces(ctx context.Context, data ptrace.Traces) error {
	publishingData, err := e.marshaller.Marshal(data)
	if err != nil {
		return err
	}

	return e.publisher.publish(ctx, publishingData)
}

func (e *rabbitMqMetricsExporter) exportMetrics(ctx context.Context, data pmetric.Metrics) error {
	publishingData, err := e.marshaller.Marshal(data)
	if err != nil {
		return err
	}

	return e.publisher.publish(ctx, publishingData)
}

func (e *rabbitMqLogsExporter) exportLogs(ctx context.Context, data plog.Logs) error {
	publishingData, err := e.marshaller.Marshal(data)
	if err != nil {
		return err
	}

	return e.publisher.publish(ctx, publishingData)
}

// TODO implement and add code re-use for other types of telemetry
func (p *rabbitMqPublisher) publish(ctx context.Context, data *publishingData) error {
	p.channelCacher.restoreConnectionIfUnhealthy()
	channelWrapper, err := p.channelCacher.requestHealthyChannelFromPool()

	if err != nil {
		return err
	}

	err, healthyChannel := p.publishWithChannelManager(ctx, data, channelWrapper)
	p.channelCacher.returnChannelToPool(channelWrapper, healthyChannel)
	return err
}

func (p *rabbitMqPublisher) publishWithChannelManager(ctx context.Context, data *publishingData, wrapper *amqpChannelManager) (err error, healthyChannel bool) {
	deliveryMode := amqp.Transient
	if p.config.durable {
		deliveryMode = amqp.Persistent
	}

	// TODO handle case where message doesn't get routed to any queues and is returned (when configured with mandatory = true)
	confirmation, err := wrapper.channel.PublishWithDeferredConfirmWithContext(ctx, "amq.direct", p.config.routingKey, false, false, amqp.Publishing{
		Headers:         amqp.Table{},
		ContentType:     data.ContentType,
		ContentEncoding: data.ContentEncoding,
		DeliveryMode:    deliveryMode,
		Body:            data.Body,
	})

	if err != nil {
		return err, false
	}

	select {
	case <-confirmation.Done():
		if confirmation.Acked() {
			p.set.Logger.Debug("Received ack", zap.Uint64("channelId", wrapper.id), zap.Uint64("deliveryTag", confirmation.DeliveryTag()))
			return nil, true
		}
		p.set.Logger.Warn("Received nack from rabbitmq publishing confirmation", zap.Uint64("channelId", wrapper.id), zap.Uint64("deliveryTag", confirmation.DeliveryTag()))
		err := errors.New("received nack from rabbitmq publishing confirmation")
		return err, true

	case <-time.After(p.config.publishConfirmationTimeout):
		p.set.Logger.Warn("Timeout waiting for publish confirmation", zap.Duration("timeout", p.config.publishConfirmationTimeout), zap.Uint64("channelId", wrapper.id), zap.Uint64("deliveryTag", confirmation.DeliveryTag()))
		err := fmt.Errorf("timeout waiting for publish confirmation after %s", p.config.publishConfirmationTimeout)
		return err, false
	}
}

func (e *rabbitMqLogsExporter) Close(context.Context) error {
	return e.publisher.channelCacher.close()
}
