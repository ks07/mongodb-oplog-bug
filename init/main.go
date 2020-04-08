package main

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"strings"
)

func main() {
	ctx := context.TODO()

	log.Println("Connecting to mongo...")
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		log.Fatal(err)
	}

	log.Println("Connected, starting session...")

	session, err := client.StartSession()
	if err != nil {
		log.Fatal(err)
	}

	if err = mongo.WithSession(ctx, session, doTx); err != nil {
		log.Fatal(err)
	}

	session.EndSession(ctx)

	if err := client.Disconnect(ctx); err != nil {
		log.Fatal(err)
	}
}

func doTx(ctx mongo.SessionContext) error {
	coll := ctx.Client().Database("foo").Collection("junk")

	var writes []mongo.WriteModel
	for i := 1; i <= 1_000_000; i++ {
		model := mongo.NewInsertOneModel()
		model = model.SetDocument(bson.D{
			{"i", i},
			{"s", strings.Repeat("F", 16)},
		})

		writes = append(writes, model)
	}

	_, err := coll.BulkWrite(ctx, writes)
	return err
}
