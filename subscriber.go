package gqlwsredis

import (
	"github.com/gomodule/redigo/redis"
	"github.com/lab259/graphqlws"
)

type redisSubscriber struct {
	conn redis.PubSubConn
}

func (subscriber *redisSubscriber) Subscribe(topic graphqlws.Topic) error {
	return subscriber.conn.Subscribe(topic)
}

func NewRedisSubscriber(conn redis.PubSubConn) *redisSubscriber {
	return &redisSubscriber{
		conn: conn,
	}
}
