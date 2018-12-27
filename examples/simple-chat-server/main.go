package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/jamillosantos/handler"
	"github.com/lab259/graphql"
	"github.com/lab259/graphqlws"
	"github.com/lab259/graphqlws-redis"
	"github.com/rs/cors"
	log "github.com/sirupsen/logrus"
	"net/http"
	"time"
)

type User struct {
	Name string `json:"name"`
}

type Message struct {
	Text string `json:"text"`
	User *User  `json:"user"`
}

func main() {
	log.SetLevel(log.InfoLevel)
	log.Info("Starting example server on :8085")

	pool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", "localhost:6379")
		},
	}
	defer pool.Close()

	conn := pool.Get()
	str, err := redis.String(conn.Do("PING", "testing connection"))
	if err != nil {
		panic(err)
	}
	if str != "testing connection" {
		panic("wrong string got from redis test")
	}

	log.Infoln("Redis initialized")

	users := make(map[string]*User)

	userType := graphql.NewObject(graphql.ObjectConfig{
		Name: "User",
		Fields: graphql.Fields{
			"name": &graphql.Field{
				Type: graphql.String,
			},
		},
	})

	messageType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Message",
		Fields: graphql.Fields{
			"text": &graphql.Field{
				Type: graphql.String,
			},
			"user": &graphql.Field{
				Type: userType,
			},
		},
	})

	rootQuery := graphql.ObjectConfig{Name: "RootQuery", Fields: graphql.Fields{
		"me": &graphql.Field{
			Type: userType,
			Resolve: func(p graphql.ResolveParams) (i interface{}, e error) {
				return &User{
					Name: "unknkown",
				}, nil
			},
		},
	}}

	var subscriptionManager graphqlws.SubscriptionManager
	var broadcastMessage func(message *Message)

	schemaConfig := graphql.SchemaConfig{
		Query: graphql.NewObject(rootQuery),
		Mutation: graphql.NewObject(graphql.ObjectConfig{
			Name: "MutationRoot",
			Fields: graphql.Fields{
				"send": &graphql.Field{
					Args: graphql.FieldConfigArgument{
						"user": &graphql.ArgumentConfig{
							Type: graphql.NewNonNull(graphql.String),
						},
						"text": &graphql.ArgumentConfig{
							Type: graphql.NewNonNull(graphql.String),
						},
					},
					Type: messageType,
					Resolve: func(p graphql.ResolveParams) (i interface{}, e error) {
						userName := p.Args["user"].(string)

						user, ok := users[userName]
						if !ok {
							return nil, fmt.Errorf("user '%s' not found", userName)
						}

						text, ok := p.Args["text"]
						if !ok {
							return nil, errors.New("you must pass the text")
						}
						m := &Message{
							Text: text.(string),
							User: user,
						}
						broadcastMessage(m)
						return m, nil
					},
				},
			},
		}),
		Subscription: graphql.NewObject(graphql.ObjectConfig{
			Name: "SubscriptionRoot",
			Fields: graphql.Fields{
				"onJoin": &graphqlws.SubscriptionField{
					Field: graphql.Field{
						Type: userType,
						Resolve: func(p graphql.ResolveParams) (i interface{}, e error) {
							userData, ok := p.Context.Value("onJoin").([]byte)
							if !ok {
								return nil, errors.New("could not get the data")
							}
							var user User
							err := json.Unmarshal(userData, &user)
							if err != nil {
								return nil, err
							}
							return user, nil
						},
					},
					Subscribe: func(subscriber graphqlws.Subscriber) error {
						return subscriber.Subscribe(graphqlws.StringTopic("onJoin"))
					},
				},
				"onLeft": &graphqlws.SubscriptionField{
					Field: graphql.Field{
						Type: userType,
						Resolve: func(p graphql.ResolveParams) (i interface{}, e error) {
							userData, ok := p.Context.Value("onLeft").([]byte)
							if !ok {
								return nil, errors.New("could not get the data")
							}
							var user User
							err := json.Unmarshal(userData, &user)
							if err != nil {
								return nil, err
							}
							return user, nil
						},
					},
					Subscribe: func(subscriber graphqlws.Subscriber) error {
						return subscriber.Subscribe(graphqlws.StringTopic("onLeft"))
					},
				},
				"onMessage": &graphqlws.SubscriptionField{
					Field: graphql.Field{
						Type: messageType,
						Resolve: func(p graphql.ResolveParams) (i interface{}, e error) {
							messageData, ok := p.Context.Value("onMessage").([]byte)
							if !ok {
								return nil, nil
							}
							var message Message
							err := json.Unmarshal(messageData, &message)
							if err != nil {
								return nil, err
							}
							return message, nil
						},
					},
					Subscribe: func(subscriber graphqlws.Subscriber) error {
						return subscriber.Subscribe(graphqlws.StringTopic("onMessage"))
					},
				},
			},
		}),
	}
	schema, err := graphql.NewSchema(schemaConfig)
	if err != nil {
		log.WithField("err", err).Panic("GraphQL schema is invalid")
	}

	// Create subscription manager and GraphQL WS handler
	subscriptionManager = gqlwsredis.NewRedisSubscriptionManager(&schema, pool, log.New())

	// Sends this to all subscriptions
	broadcastJoin := func(user *User) {
		err := subscriptionManager.Publish(graphqlws.StringTopic("onJoin"), user)
		if err != nil {
			log.Errorln(err.Error())
		}
	}

	broadcastLeft := func(user *User) {
		err := subscriptionManager.Publish(graphqlws.StringTopic("onLeft"), user)
		if err != nil {
			log.Errorln(err.Error())
		}
	}

	broadcastMessage = func(message *Message) {
		err := subscriptionManager.Publish(graphqlws.StringTopic("onMessage"), message)
		if err != nil {
			log.Errorln(err.Error())
		}
	}

	connFactory := gqlwsredis.NewRedisConnectionCreator(&schema, pool, graphqlws.ConnectionConfig{
		Authenticate: func(token string) (interface{}, error) {
			if token == "Anonymous" {
				return nil, errors.New("forbidden")
			}
			user := &User{
				Name: token,
			}
			log.Infoln(token, "joined")
			users[user.Name] = user
			broadcastJoin(user)
			return user, nil
		},
		EventHandlers: graphqlws.ConnectionEventHandlers{
			Close: func(connection graphqlws.Connection) {
				u := connection.User().(*User)
				broadcastLeft(u)
				log.Infoln(u.Name, "left")
				delete(users, u.Name)
			},
		},
	})

	websocketHandler := graphqlws.NewHandler(graphqlws.HandlerConfig{
		Schema:              &schema,
		ConnectionFactory:   connFactory,
		SubscriptionManager: subscriptionManager,
	})

	h := handler.New(&handler.Config{
		Schema:     &schema,
		Pretty:     true,
		GraphiQL:   false,
		Playground: true,
	})

	// Serve the GraphQL WS endpoint
	mux := http.NewServeMux()
	mux.Handle("/graphql", h)
	mux.Handle("/subscriptions", websocketHandler)
	if err := http.ListenAndServe(":8085", cors.AllowAll().Handler(mux)); err != nil {
		log.WithField("err", err).Error("Failed to start server")
	}
}
