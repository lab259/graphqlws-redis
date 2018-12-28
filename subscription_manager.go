package gqlwsredis

import (
	"errors"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/lab259/graphql"
	"github.com/lab259/graphqlws"
	log "github.com/sirupsen/logrus"
	"strings"
)

// redisSubscriptionManager implements the `graphqlws.SubscriptionManager` for
// the redis PubSub system.
type redisSubscriptionManager struct {
	schema *graphql.Schema
	pool   *redis.Pool
	logger *log.Logger
}

// NewRedisSubscriptionManager
func NewRedisSubscriptionManager(schema *graphql.Schema, pool *redis.Pool, logger *log.Logger) graphqlws.SubscriptionManager {
	return &redisSubscriptionManager{
		schema: schema,
		pool:   pool,
		logger: logger,
	}
}

// AddSubscription open a new connection with the redis
func (m *redisSubscriptionManager) AddSubscription(conn graphqlws.Connection, subscription graphqlws.SubscriptionInterface) []error {
	subscriber := graphqlws.NewInMemorySubscriber(subscription)

	result := make([]error, 0)

	var fields graphql.Fields
	switch fs := m.schema.SubscriptionType().TypedConfig().Fields.(type) {
	case graphql.Fields:
		fields = fs
	case graphql.FieldsThunk:
		fields = fs()
	default:
		result = append(result, errors.New("fields type not supported"))
		return result
	}

	log.WithFields(log.Fields{
		"connID":         subscription.GetConnection().ID(),
		"subscriptionID": subscription.GetID(),
		"fields":         strings.Join(subscription.GetFields(), ", "),
	}).Infoln("subscribing")

	for _, fieldName := range subscription.GetFields() {
		field, ok := fields[fieldName]
		if !ok {
			panic(fmt.Sprintf("subscription %s not found", fieldName))
		}
		subscriptionField, ok := field.(*graphqlws.SubscriptionField)
		if !ok {
			panic(fmt.Sprintf("subscription %s is not a SubscriptionField", fieldName))
		}
		err := subscriptionField.Subscribe(subscriber)
		if err != nil {
			result = append(result, err)
			continue
		}
	}
	err := m.Subscribe(subscriber)
	if err != nil {
		result = append(result, err)
	}

	if len(result) > 0 {
		return result
	}
	return nil
}

// RemoveSubscription removes a subscription from the connection.
func (m *redisSubscriptionManager) RemoveSubscription(conn graphqlws.Connection, subscriptionID string) {
	//
}

func (m *redisSubscriptionManager) RemoveSubscriptions(conn graphqlws.Connection) {
	//
}

func (m *redisSubscriptionManager) CreateSubscriptionSubscriber(subscription graphqlws.SubscriptionInterface) graphqlws.Subscriber {
	return graphqlws.NewInMemorySubscriber(subscription)
}

func (m *redisSubscriptionManager) Publish(topic graphqlws.Topic, data interface{}) error {
	conn := m.pool.Get()
	_, err := conn.Do("PUBLISH", topic, data)
	m.logger.WithField("topic", topic).WithField("data", data).Infoln("publishing to topic")
	return err
}

func (m *redisSubscriptionManager) Subscribe(subscriber graphqlws.Subscriber) error {
	conn, ok := subscriber.Subscription().GetConnection().(*redisConnection)
	if !ok {
		return errors.New("the conn is not a `*redisConnection`")
	}
	rConn := m.pool.Get()
	psConn := redis.PubSubConn{Conn: rConn}
	topics := subscriber.Topics()
	channels := make([]interface{}, len(topics))
	for i, topic := range topics {
		channels[i] = topic
	}
	err := psConn.Subscribe(channels...)
	log.Infoln("Subscribing to ", channels)
	if err != nil {
		return err
	}
	conn.startSubscriptionRedisConn(subscriber.Subscription(), &psConn)
	return nil
}
