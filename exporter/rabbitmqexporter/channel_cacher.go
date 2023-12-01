package rabbitmqexporter

import (
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/rabbitmqexporter/internal"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
	"sync"
	"time"
)

// The channel cacher is used to gain exclusive access to an AMQP 0.9.1 channel for publishing queue messages, since they are not thread safe.
// AMQP channels are like logical connections that share a single physical connection to reduce rabbitmq resource usage.
//
// This struct wraps a single connection and a fixed number of AMQP channels.
// Re-using channels between batches avoids a few network calls for closing/recreating the channel (which took ~50ms in local testing with the nearest AWS region).
// It also handles lazily recreating unhealthy connections/channels when a new batch comes in.

// Much of this implementation is adapted from https://github.com/houseofcat/turbocookedrabbit's connection pool.
type amqpChannelCacher struct {
	logger             *zap.Logger
	config             *connectionConfig
	amqpClient         internal.AmqpClient
	connection         internal.WrappedConnection
	connLock           *sync.Mutex
	channelManagerPool chan *amqpChannelManager
	connectionErrors   chan *amqp.Error
}

type connectionConfig struct {
	logger            *zap.Logger
	connectionUrl     string
	connectionName    string
	channelPoolSize   int
	heartbeatInterval time.Duration
	connectionTimeout time.Duration
	confirmationMode  bool
}

type amqpChannelManager struct {
	id         int
	channel    internal.WrappedChannel
	wasHealthy bool
	lock       *sync.Mutex
	logger     *zap.Logger
}

func newAmqpChannelCacher(config *connectionConfig, amqpClient internal.AmqpClient) (*amqpChannelCacher, error) {
	acc := &amqpChannelCacher{
		logger:             config.logger,
		config:             config,
		amqpClient:         amqpClient,
		connLock:           &sync.Mutex{},
		channelManagerPool: make(chan *amqpChannelManager, config.channelPoolSize),
	}

	err := acc.connect()
	if err != nil {
		return nil, err
	}

	// Synchronously try creating and connecting to channels
	for i := 0; i < acc.config.channelPoolSize; i++ {
		acc.channelManagerPool <- acc.createChannelManager(i)
	}

	return acc, nil
}

func (acc *amqpChannelCacher) connect() error {
	acc.logger.Debug("Connecting to rabbitmq")

	if acc.connection != nil && !acc.connection.IsClosed() {
		acc.logger.Debug("Already connected before acquiring lock")
		return nil
	}

	acc.connLock.Lock()
	defer acc.connLock.Unlock()

	// Recompare, check if an operation is still necessary after acquiring lock.
	if acc.connection != nil && !acc.connection.IsClosed() {
		acc.logger.Debug("Already connected after acquiring lock")
		return nil
	}

	// Proceed with re-connecting
	var amqpConn internal.WrappedConnection
	var err error

	amqpConn, err = acc.amqpClient.DialConfig(acc.config.connectionUrl, amqp.Config{
		Heartbeat: acc.config.heartbeatInterval,
		Dial:      acc.amqpClient.DefaultDial(acc.config.connectionTimeout),
		Properties: amqp.Table{
			"connection_name": acc.config.connectionName,
		},
		// TODO TLS config
	})
	if err != nil {
		return err
	}

	acc.connection = amqpConn

	// Goal is to lazily restore the connection so this needs to be buffered to avoid blocking on asynchronous amqp errors.
	// Also re-create this channel each time because apparently the amqp library can close it
	acc.connectionErrors = make(chan *amqp.Error, 1)
	acc.connection.NotifyClose(acc.connectionErrors)

	// TODO handle upstream flow control throttling publishing
	//acc.Blockers = make(chan amqp.Blocking, 10)
	//acc.connection.NotifyBlocked(acc.Blockers)

	return nil
}

func (acc *amqpChannelCacher) restoreConnectionIfUnhealthy() {
	healthy := true
	select {
	case err := <-acc.connectionErrors:
		healthy = false
		acc.logger.Debug("Received connection error, will retry restoring unhealthy connection", zap.Error(err))
	default:
		break
	}

	if !healthy || acc.connection.IsClosed() {
		if !acc.connection.IsClosed() {
			err := acc.close()
			if err != nil {
				acc.logger.Warn("Error closing unhealthy connection", zap.Error(err))
			}
		}

		if err := acc.connect(); err != nil {
			acc.logger.Warn("Failed attempt at restoring unhealthy connection", zap.Error(err))
		} else {
			acc.logger.Info("Restored unhealthy connection")
		}
	}
}

func (acc *amqpChannelCacher) createChannelManager(id int) *amqpChannelManager {
	channelWrapper := &amqpChannelManager{id: id, logger: acc.logger, lock: &sync.Mutex{}}
	err := channelWrapper.tryReplacingChannel(acc.connection, acc.config.confirmationMode)
	if err != nil {
		acc.logger.Warn("Error creating channel manager's channel", zap.Error(err))
	}
	return channelWrapper
}

func (acc *amqpChannelCacher) requestHealthyChannelFromPool() (*amqpChannelManager, error) {
	channelWrapper := <-acc.channelManagerPool
	if !channelWrapper.wasHealthy {
		err := acc.reconnectChannel(channelWrapper)
		if err != nil {
			acc.returnChannelToPool(channelWrapper, false)
			return nil, err
		}
	}
	return channelWrapper, nil
}

func (acc *amqpChannelCacher) returnChannelToPool(channelWrapper *amqpChannelManager, wasHealthy bool) {
	channelWrapper.wasHealthy = wasHealthy
	acc.channelManagerPool <- channelWrapper
	return
}

func (acc *amqpChannelCacher) reconnectChannel(channel *amqpChannelManager) error {
	acc.restoreConnectionIfUnhealthy()
	return channel.tryReplacingChannel(acc.connection, acc.config.confirmationMode)
}

func (acw *amqpChannelManager) tryReplacingChannel(connection internal.WrappedConnection, confirmAcks bool) error {
	acw.lock.Lock()
	defer acw.lock.Unlock()

	if acw.channel != nil && !acw.channel.IsClosed() {
		err := acw.channel.Close()
		if err != nil {
			acw.logger.Debug("Error closing existing channel", zap.Error(err))
			acw.wasHealthy = false
			return err
		}
	}

	var err error
	acw.channel, err = connection.Channel()
	if err != nil {
		acw.logger.Warn("Channel creation error", zap.Error(err))
		acw.wasHealthy = false
		return err
	}

	if confirmAcks {
		err := acw.channel.Confirm(false)
		if err != nil {
			acw.logger.Debug("Error entering confirm mode", zap.Error(err))
			acw.wasHealthy = false
			return err
		}
	}

	acw.wasHealthy = true
	return nil
}

func (acc *amqpChannelCacher) close() error {
	err := acc.connection.Close()
	if err != nil {
		acc.logger.Warn("Error closing connection", zap.Error(err))
		if err != amqp.ErrClosed {
			return err
		}
	}
	return nil
}
