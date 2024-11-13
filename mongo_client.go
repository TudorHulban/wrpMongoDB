package mongoclient

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// Package mongoclient is sandbox for mongo go driver.
// Used examples as per  https://kb.objectrocket.com/mongo-db/how-to-update-a-mongodb-document-using-the-golang-driver-458.

// Local context timeouts use global cfg but could be passed as argument if needed.
// Returning primitive.ObjectID which is a byte array.
// TODO: verify objID, _ := primitive.ObjectIDFromHex(id) transformation

type Cfg struct {
	URL        string
	Database   string
	Collection string

	SecondsTimeoutExecution uint
}

type Client struct {
	*Cfg

	client *mongo.Client
}

type record struct {
	Name   string
	Gender string
	Age    uint
}

// NewMongo Constructor for Mongo client.
// Caller would need to handle connect / disconnect.
func NewMongo(config *Cfg) (*Client, error) {
	if config == nil {
		// TODO: add use of default configuration.
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Duration(config.SecondsTimeoutExecution)*time.Second,
	)
	defer cancel()

	instance, errConnect := mongo.Connect(
		ctx,
		options.Client().ApplyURI(config.URL),
	)
	if errConnect != nil {
		return nil, errConnect
	}

	if errPing := instance.Ping(ctx, readpref.Primary()); errPing != nil {
		return nil,
			errPing
	}

	result, errClient := mongo.NewClient(options.Client().ApplyURI(config.URL))
	if errClient != nil || result == nil {
		return nil,
			errClient
	}

	return &Client{
			Cfg:    config,
			client: result,
		},
		nil
}

// Connect Method connects client instance to configured database.
func (m *Client) Connect(ctx context.Context) error {
	return m.client.Connect(ctx)
}

// Disconnect Method disconnects client from database.
func (m *Client) Disconnect(ctx context.Context) error {
	return m.client.Disconnect(ctx)
}

// InsertOne Method inserts the data and returns the ID of the inserted data and error.
func (m *Client) InsertOne(ctx context.Context, data []byte) (primitive.ObjectID, error) {
	ctxLocal, cancel := context.WithTimeout(ctx, time.Duration(m.SecondsTimeoutExecution)*time.Second)
	defer cancel()

	dataM, errConv := jsonToBsonM(data)
	if errConv != nil {
		return primitive.ObjectID{}, errConv
	}

	collection := m.client.Database(m.Database).Collection(m.Collection)
	if collection == nil {
		return primitive.ObjectID{},
			errors.New("collection is nil")
	}

	result, errInsert := collection.InsertOne(ctxLocal, dataM)
	if errInsert != nil || result == nil {
		return primitive.ObjectID{},
			errInsert
	}

	return result.InsertedID.(primitive.ObjectID),
		nil
}

// FindOne Method finds data based on passed filter and returns it.
func (m *Client) FindOne(ctx context.Context, filter []byte) (any, error) {
	ctxLocal, cancel := context.WithTimeout(
		ctx,
		time.Duration(m.SecondsTimeoutExecution)*time.Second,
	)
	defer cancel()

	bsonFilter, errConv := jsonToBsonM(filter)
	if errConv != nil {
		return nil, errConv
	}

	var result bson.M

	if errFind := m.client.
		Database(m.Database).
		Collection(m.Collection).
		FindOne(
			ctxLocal,
			bsonFilter,
		).
		Decode(&result); errFind != nil {
		return nil,
			errFind
	}

	return result,
		nil
}

func (m *Client) FindByID(ctx context.Context, objectID primitive.ObjectID) (any, error) {
	ctxLocal, cancel := context.WithTimeout(
		ctx,
		time.Duration(m.Cfg.SecondsTimeoutExecution)*time.Second,
	)
	defer cancel()

	bsonFilter := bson.M{"_id": bson.M{"$eq": objectID}} // variable not needed, inject directly

	var result bson.M
	errFind := m.client.
		Database(m.Database).
		Collection(m.Cfg.Collection).
		FindOne(
			ctxLocal,
			bsonFilter,
		).
		Decode(&result)
	if errFind != nil {
		return nil,
			errFind
	}

	return result, // ex. "5d678d799139918d230cfd41"
		nil
}

// FindManyFilterJSON Method finds data based on passed ID and returns it. Could return more than one record.
func (m *Client) FindManyFilterJSON(ctx context.Context, filterJSON []byte) ([]bson.M, error) {
	ctxLocal, cancel := context.WithTimeout(
		ctx,
		time.Duration(m.Cfg.SecondsTimeoutExecution)*time.Second,
	)
	defer cancel()

	bsonFilter, errConv := jsonToBsonM(filterJSON)
	if errConv != nil {
		return nil,
			errConv
	}

	cursor, errFind := m.client.
		Database(m.Cfg.Database).
		Collection(m.Cfg.Collection).
		Find(
			ctxLocal,
			bsonFilter,
		)
	if errFind != nil {
		return nil,
			errFind
	}
	defer cursor.Close(ctxLocal)

	return walkMongoSet(ctxLocal, cursor)
}

// FindManyFilterBSON Method finds data based on passed ID and returns it. Could return more than one record.
func (m *Client) FindManyFilterBSON(ctx context.Context, filterBSON primitive.M) ([]bson.M, error) {
	ctxLocal, cancel := context.WithTimeout(ctx, time.Duration(m.Cfg.SecondsTimeoutExecution)*time.Second)
	defer cancel()

	cursor, errFind := m.client.
		Database(m.Cfg.Database).
		Collection(m.Cfg.Collection).
		Find(
			ctxLocal,
			filterBSON,
		)
	if errFind != nil {
		return nil,
			errFind
	}
	defer cursor.Close(ctxLocal)

	return walkMongoSet(ctxLocal, cursor)
}

func walkMongoSet(ctx context.Context, cursor *mongo.Cursor) ([]bson.M, error) {
	var result []bson.M

	for cursor.Next(ctx) {
		var buf bson.M

		if errDecode := cursor.Decode(&buf); errDecode != nil {
			return nil, errors.Wrap(errDecode, "could not decode into buffer")
		}
		result = append(result, buf)
	}

	if errCursor := cursor.Err(); errCursor != nil {
		return nil,
			errors.Wrap(errCursor, "cursor error")
	}

	return result,
		nil
}

// DeleteOne Method deletes one record from found.
func (m *Client) DeleteOne(ctx context.Context, filter []byte) (any, error) {
	ctxLocal, cancel := context.WithTimeout(ctx, time.Duration(m.Cfg.SecondsTimeoutExecution)*time.Second)
	defer cancel()

	bsonFilter, errConv := jsonToBsonM(filter)
	if errConv != nil {
		return nil,
			errConv
	}

	return m.client.
		Database(m.Cfg.Database).
		Collection(m.Cfg.Collection).
		DeleteOne(ctxLocal, bsonFilter)
}

// DeleteAll Method deletes all records found matching passed filter.
func (m *Client) DeleteAll(ctx context.Context, filter []byte) (any, error) {
	ctxLocal, cancel := context.WithTimeout(ctx, time.Duration(m.SecondsTimeoutExecution)*time.Second)
	defer cancel()

	bsonFilter, errConv := jsonToBsonM(filter)
	if errConv != nil {
		return nil,
			errConv
	}

	return m.client.
		Database(m.Cfg.Database).
		Collection(m.Cfg.Collection).
		DeleteMany(ctxLocal, bsonFilter)
}

// UpdateByID Method updates record with passed ID.
func (m *Client) UpdateByID(ctx context.Context, id primitive.ObjectID, newValue bson.M) (any, error) {
	ctxLocal, cancel := context.WithTimeout(ctx, time.Duration(m.SecondsTimeoutExecution)*time.Second)
	defer cancel()

	return m.client.
		Database(m.Database).
		Collection(m.Collection).
		UpdateOne(
			ctxLocal,
			bson.M{"_id": bson.M{"$eq": id}},
			newValue,
		)
}

// UpdateOne Method updates one record from those matching passed filter.
func (m *Client) UpdateOne(ctx context.Context, filter primitive.M, newValue bson.M) (any, error) {
	ctxLocal, cancel := context.WithTimeout(ctx, time.Duration(m.SecondsTimeoutExecution)*time.Second)
	defer cancel()

	return m.client.
		Database(m.Database).
		Collection(m.Collection).
		UpdateOne(ctxLocal, filter, newValue)
}

// UpdateMany Method updates all records that match the passed filter search.
func (m *Client) UpdateMany(ctx context.Context, filter []byte, newValue bson.M) (any, error) {
	ctxLocal, cancel := context.WithTimeout(ctx, time.Duration(m.SecondsTimeoutExecution)*time.Second)
	defer cancel()

	bsonFilter, errConv := jsonToBsonM(filter)
	if errConv != nil {
		return nil,
			errConv
	}

	return m.client.
		Database(m.Database).
		Collection(m.Collection).
		UpdateMany(ctxLocal, bsonFilter, newValue)
}
