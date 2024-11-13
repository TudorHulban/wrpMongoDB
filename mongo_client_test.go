package mongoclient

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/TudorHulban/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var (
	john = record{
		Name:   "john",
		Gender: "male",
		Age:    44,
	}

	mary = record{
		Name:   "mary",
		Gender: "female",
		Age:    44,
	}

	_logger = log.New(log.DEBUG, os.Stderr, true)
)

func testCfg() *Cfg {
	return &Cfg{
		SecondsTimeoutExecution: 3,
		URL:                     "mongodb://localhost:27017",
		Database:                "testing",
		Collection:              "persons",
	}
}

func testInsertOne(ctx context.Context, t *testing.T, m *Client, value record) primitive.ObjectID {
	valueMarshalled, errMarshall := json.Marshal(value)
	require.NoError(t, errMarshall)

	id, errInsert := m.InsertOne(ctx, valueMarshalled)
	require.NoError(t, errInsert)
	require.NotEmpty(t, id)

	_logger.Printf(
		"inserted ID: %s", id.String(),
	)

	return id
}

func TestInsertOne(t *testing.T) {
	client, errNew := NewMongo(testCfg())
	require.NoError(t,
		errNew,
		"connection to Mongo DB issues",
	)
	require.NotNil(t, client)

	ctx := context.Background()

	require.NoError(t,
		client.Connect(ctx),
		"could not connect",
	)
	defer client.Disconnect(ctx)

	require.NotEmpty(t,
		testInsertOne(ctx, t, client, john),
	)
}

func TestFindByID(t *testing.T) {
	client, errNew := NewMongo(testCfg())
	require.NoError(t, errNew, "connection to Mongo DB issues")
	require.NotNil(t, client)

	ctx := context.Background()
	require.NoError(t,
		client.Connect(ctx),
		"could not connect",
	)
	defer client.Disconnect(ctx)

	johnID := testInsertOne(ctx, t, client, john)

	record, errFindID := client.FindByID(ctx, johnID)
	require.NoError(t, errFindID, "error find by ID")
	require.NotNil(t, record)

	_logger.Printf("found (1): %s", record)
}

func TestFindOneRecord(t *testing.T) {
	m, errNew := NewMongo(testCfg())
	require.NoError(t, errNew, "connection to Mongo DB issues")
	require.NotNil(t, m)

	ctx := context.Background()
	require.NoError(t, m.Connect(ctx), "could not connect")
	defer m.Disconnect(ctx)

	jsonJohn, errMarshall := json.Marshal(john)
	require.Nil(t, errMarshall)

	record, errFind := m.FindOne(ctx, jsonJohn)
	require.NoError(t, errFind)
	require.NotNil(t, record)
}

func TestFindOneNoRecord(t *testing.T) {
	m, errNew := NewMongo(testCfg())
	require.NoError(t, errNew, "connection to Mongo DB issues")
	require.NotNil(t, m)

	ctx := context.Background()
	require.NoError(t, m.Connect(ctx), "could not connect")
	defer m.Disconnect(ctx)

	// TODO: implementation.
}

// TestFindMany Should find several records.
func TestFindManyJSON(t *testing.T) {
	m, errNew := NewMongo(testCfg())
	require.NoError(t, errNew, "connection to Mongo DB issues")
	require.NotNil(t, m)

	ctx := context.Background()
	require.NoError(t, m.Connect(ctx), "could not connect")
	defer m.Disconnect(ctx)

	testInsertOne(ctx, t, m, mary)

	age := struct{ age uint }{
		age: 44,
	}
	jsonAge, errMarshall := json.Marshal(age)
	require.NoError(t, errMarshall)

	record, errMany := m.FindManyFilterJSON(ctx, jsonAge)
	require.NoError(t, errMany)
	assert.Greater(t, len(record), 2)

	for k, v := range record {
		_logger.Print(k, v)
	}
}

// TestDeleteOne Should delete one record provided the passed filter.
func TestDeleteOne(t *testing.T) {
	m, errNew := NewMongo(testCfg())
	require.NoError(t, errNew, "connection to Mongo DB issues")
	require.NotNil(t, m)

	ctx := context.Background()
	require.NoError(t, m.Connect(ctx), "could not connect")
	defer m.Disconnect(ctx)

	jsonMary, errMarshall := json.Marshal(mary)
	require.NoError(t, errMarshall)

	manyMaryBefore, errMany := m.FindManyFilterJSON(ctx, jsonMary)

	_, errDelete := m.DeleteOne(ctx, jsonMary)
	require.NoError(t, errDelete)

	manyMaryAfter, errMany := m.FindManyFilterJSON(ctx, jsonMary)
	require.NoError(t, errMany)
	assert.Equal(t, len(manyMaryAfter), len(manyMaryBefore)-1)
}

// TestDeleteAll Should delete all records for passed filter.
func TestDeleteAll(t *testing.T) {
	m, errNew := NewMongo(testCfg())
	require.NoError(t, errNew, "connection to Mongo DB issues")
	require.NotNil(t, m)

	ctx := context.Background()
	require.NoError(t, m.Connect(ctx), "could not connect")
	defer m.Disconnect(ctx)

	raw, errMarshall := json.Marshal(john)
	require.NoError(t, errMarshall)

	_, errDelete := m.DeleteAll(ctx, raw)
	require.NoError(t, errDelete)

	many, errMany := m.FindManyFilterJSON(ctx, raw)
	require.NoError(t, errMany)
	assert.Equal(t, len(many), 0)
}

// TestUpdateByID Should update the record with passed ID.
func TestUpdateByID(t *testing.T) {
	m, errNew := NewMongo(testCfg())
	require.NoError(t, errNew, "connection to Mongo DB issues")
	require.NotNil(t, m)

	ctx := context.Background()
	require.NoError(t, m.Connect(ctx), "could not connect")
	defer m.Disconnect(ctx)

	id := testInsertOne(ctx, t, m, mary)
	bsonUpdate := bson.M{"$set": bson.M{"Name": "mary", "Gender": "female", "Age": 45}} // just changing the age

	_, errUpdate := m.UpdateByID(ctx, id, bsonUpdate)
	require.Nil(t, errUpdate)

	maryUpdated := record{
		Name:   "mary",
		Gender: "female",
		Age:    45,
	}

	raw, errMarshall := json.Marshal(maryUpdated)
	require.NoError(t, errMarshall)

	record, errFind := m.FindOne(ctx, raw)
	require.NoError(t, errFind)
	require.NotNil(t, record)
}

// TestUpdateOne Should update a record given passed data.
func TestUpdateOne(t *testing.T) {
	m, errNew := NewMongo(testCfg())
	require.NoError(t, errNew, "connection to Mongo DB issues")
	require.NotNil(t, m)

	ctx := context.Background()
	require.Nil(t, m.Connect(ctx), "could not connect")
	defer m.Disconnect(ctx)

	testInsertOne(ctx, t, m, mary)

	// define filter for the value to be updated
	bsonFilter := bson.M{
		"name": bson.M{
			"$eq": "mary",
		},
	}

	// define data to update with
	bsonUpdate := bson.M{
		"$set": bson.M{
			"age": 55,
		},
	}

	_, errUpdate := m.UpdateOne(ctx, bsonFilter, bsonUpdate)
	require.NoError(t, errUpdate)
}
