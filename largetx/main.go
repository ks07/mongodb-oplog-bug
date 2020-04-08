package main

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"math/rand"
	"strings"
	"time"
)

func main() {
	ctx := context.TODO()

	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		log.Fatal(err)
	}

	rand.Seed(time.Now().UTC().UnixNano())

	runOnce(ctx, client)

	if err := client.Disconnect(ctx); err != nil {
		log.Fatal(err)
	}
}

func runOnce(ctx context.Context, client *mongo.Client) {
	session, err := client.StartSession()
	if err != nil {
		log.Fatal(err)
	}

	if err := session.StartTransaction(); err != nil {
		log.Fatal(err)
	}

	if err = mongo.WithSession(ctx, session, doTx); err != nil {
		log.Fatal(err)
	}

	if err := session.CommitTransaction(ctx); err != nil {
		log.Fatal(err)
	}

	session.EndSession(ctx)
}

func doTx(ctx mongo.SessionContext) error {
	coll := ctx.Client().Database("foo").Collection("junk")

	var writes []mongo.WriteModel
	for i := 1; i <= 5; i++ {
		model := mongo.NewReplaceOneModel()
		model = model.SetFilter(bson.D{
			{"_id", i},
		})
		model = model.SetReplacement(bson.D{
			{"_id", i},
			{"r", rand.Float64()},
			{"s", strings.Repeat("a", 16*1024*1024-75)},
		})
		model = model.SetUpsert(true)

		writes = append(writes, model)
	}

	_, err := coll.BulkWrite(ctx, writes)
	return err
}
