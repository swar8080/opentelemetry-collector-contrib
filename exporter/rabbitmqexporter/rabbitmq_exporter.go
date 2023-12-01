package rabbitmqexporter

import (
	"context"
	"errors"
	"fmt"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/rabbitmqexporter/internal"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
	"time"
)

type rabbitMqPublisher struct {
	set           component.TelemetrySettings
	config        config
	amqpClient    *internal.AmqpClient
	channelCacher *amqpChannelCacher
}

type rabbitMqLogsExporter struct {
	publisher  *rabbitMqPublisher
	marshaller LogsMarshaler
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

	err, healthyChannel := p.publishWithWrapper(ctx, data, channelWrapper)
	p.channelCacher.returnChannelToPool(channelWrapper, healthyChannel)
	return err
}

func (p *rabbitMqPublisher) publishWithWrapper(ctx context.Context, data *publishingData, wrapper *amqpChannelManager) (err error, healthyChannel bool) {
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
			p.set.Logger.Debug("Received ack", zap.Int("channelId", wrapper.id), zap.Uint64("deliveryTag", confirmation.DeliveryTag()))
			return nil, true
		}
		p.set.Logger.Warn("Received nack from rabbitmq publishing confirmation", zap.Uint64("deliveryTag", confirmation.DeliveryTag()))
		err := errors.New("received nack from rabbitmq publishing confirmation")
		return err, true

	case <-time.After(p.config.publishConfirmationTimeout):
		p.set.Logger.Warn("Timeout waiting for publish confirmation", zap.Duration("timeout", p.config.publishConfirmationTimeout), zap.Uint64("deliveryTag", confirmation.DeliveryTag()))
		err := fmt.Errorf("timeout waiting for publish confirmation after %s", p.config.publishConfirmationTimeout)
		return err, false
	}
}

func (e *rabbitMqLogsExporter) Close(context.Context) error {
	return e.publisher.channelCacher.close()
}
