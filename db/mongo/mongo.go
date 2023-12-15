package mongo

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"time"
)

const (
	TimeoutDurationInSeconds = 10
	ConnectionPoolSize       = 10
)

func NewClient(log *zap.SugaredLogger, mongoUri string) (*mongo.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), TimeoutDurationInSeconds*time.Second)
	defer cancel()
	connectionOptions := options.Client().ApplyURI(mongoUri)
	connectionOptions.SetMaxPoolSize(ConnectionPoolSize)
	client, err := mongo.Connect(ctx, connectionOptions)
	if err != nil {
		log.Errorw(err.Error())
		return nil, err
	}
	return client, nil
}
