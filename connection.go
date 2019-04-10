package gqlwsredis

import (
	"context"
	"fmt"
	"io"

	"github.com/gomodule/redigo/redis"
	"github.com/gorilla/websocket"
	"github.com/lab259/graphql"
	"github.com/lab259/graphqlws"
	log "github.com/sirupsen/logrus"
)

const CONNECTION = "graphqlws-connection"

type redisConnection struct {
	manager          graphqlws.SubscriptionManager
	conn             graphqlws.Connection
	schema           *graphql.Schema
	redisConnections map[string]*redis.PubSubConn
}

func (conn *redisConnection) ID() string {
	return conn.conn.ID()
}

func (conn *redisConnection) Conn() *websocket.Conn {
	return conn.conn.Conn()
}

func (conn *redisConnection) User() interface{} {
	return conn.conn.User()
}

func (conn *redisConnection) SendData(d string, payload *graphqlws.DataMessagePayload) {
	conn.conn.SendData(d, payload)
}

func (conn *redisConnection) SendError(err error) {
	conn.conn.SendError(err)
}

func (conn *redisConnection) readRedis(subscription graphqlws.SubscriptionInterface, rConn *redis.PubSubConn) {
	// Register subscription ID as running, note that the registered is a
	// pointer to a bool. This way we will be able to set the value in other
	// thread and get this one stopped.
	conn.redisConnections[subscription.GetID()] = rConn
	defer rConn.Close()

	logger := log.New().WithField("method", "readRedis").WithField("connID", conn.ID()).WithField("subscription", subscription.GetID())
	logger.Infoln("starting readRedis")
	defer func() {
		logger.Infoln("exiting readRedis")
	}()

	for {
		// Wait for receiving data from the redis channels.
		// If the connection is closed, the system returns a io.EOF.
		logger.Infoln("waiting for receiving a message from Redis")
		r := rConn.Receive()
		switch response := r.(type) {
		case redis.Message:
			logger.Infoln("message received", string(response.Data))
			// In case a messages arrives
			ctx := context.WithValue(context.Background(), response.Channel, response.Data)
			ctx = context.WithValue(ctx, CONNECTION, conn)
			params := graphql.ExecuteParams{
				Schema:        *conn.schema,
				AST:           subscription.GetDocument(),
				Args:          subscription.GetVariables(),
				OperationName: subscription.GetOperationName(),
				Context:       ctx,
			}
			r := graphql.Execute(params)

			subscription.GetSendData()(&graphqlws.DataMessagePayload{
				Data:   r.Data,
				Errors: graphqlws.ErrorsFromGraphQLErrors(r.Errors),
			})
		case redis.Subscription:
			logger.Infoln("subscription received. Ignoring...")
			// If other threads subscribe to the same topic, this channel gets
			// notified.
			// TODO Decide what to do here
		case error:
			logger.WithError(response).Errorln("error receiving a Redis message")
			// If any error happens
			if response == io.EOF {
				// This error means that another goroutine closed the connection
				return
			} else if response.Error() == "redigo: connection closed" {
				// The connection was closed...
				// TODO To decide what to do in this case.
				return
			}
			// TODO What should we do in case of error, probably add an error
			//  listener
			//
			// panic(response)
		default:
			// This is unexpected... But not necessarily an error.
			// TODO Decide what to do here.
		}
	}
}

func (conn *redisConnection) startSubscriptionRedisConn(subscription graphqlws.SubscriptionInterface, rConn *redis.PubSubConn) {
	go conn.readRedis(subscription, rConn)
}

type redisConnectionFactory struct {
	schema *graphql.Schema
	pool   RedisPool
	config graphqlws.ConnectionConfig
}

// NewRedisConnectionCreator will return a function that creates connections
// based on the `redis.Pool`.
func NewRedisConnectionCreator(schema *graphql.Schema, redisPool RedisPool, config graphqlws.ConnectionConfig) graphqlws.ConnectionFactory {
	return &redisConnectionFactory{
		schema: schema,
		pool:   redisPool,
		config: config,
	}
}

func (factory *redisConnectionFactory) Create(wsConn *websocket.Conn, handlers graphqlws.ConnectionEventHandlers) graphqlws.Connection {
	redisConn, err := factory.pool.GetConn()
	if err != nil {
		// TODO Decide what to do ...
		panic(fmt.Sprintf("not implemented: %s", redisConn.Err().Error()))
	}
	if redisConn.Err() != nil {
		// TODO Decide what to do ...
		panic(fmt.Sprintf("not implemented: %s", redisConn.Err().Error()))
	}

	var rConn *redisConnection
	rConn = &redisConnection{
		schema:           factory.schema,
		redisConnections: make(map[string]*redis.PubSubConn, 5),
		conn: graphqlws.NewConnection(wsConn, graphqlws.ConnectionConfig{
			ReadLimit: factory.config.ReadLimit,
			EventHandlers: graphqlws.ConnectionEventHandlers{
				Close: func(_ graphqlws.Connection) {
					if factory.config.EventHandlers.Close != nil {
						factory.config.EventHandlers.Close(rConn)
					}
					handlers.Close(rConn)
					for subscriptionID, redisPubSubConn := range rConn.redisConnections {
						redisPubSubConn.Close()
						log.WithField("connID", rConn.ID()).WithField("subscription", subscriptionID).Infoln("set running = false")
					}
				},
				StopOperation: func(_ graphqlws.Connection, subscriptionID string) {
					if factory.config.EventHandlers.StopOperation != nil {
						factory.config.EventHandlers.StopOperation(rConn, subscriptionID)
					}
					handlers.StopOperation(rConn, subscriptionID)
				},
				StartOperation: func(_ graphqlws.Connection, subscriptionID string, payload *graphqlws.StartMessagePayload) []error {
					if factory.config.EventHandlers.StartOperation != nil {
						factory.config.EventHandlers.StartOperation(rConn, subscriptionID, payload)
					}
					return handlers.StartOperation(rConn, subscriptionID, payload)
				},
			},
			Authenticate:           factory.config.Authenticate,
			ControlMessageHandlers: factory.config.ControlMessageHandlers,
		}),
	}
	return rConn
}
