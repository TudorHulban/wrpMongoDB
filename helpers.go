package mongoclient

import (
	"encoding/json"

	"go.mongodb.org/mongo-driver/bson"
)

// jsonToBsonM converts raw JSON into binary JSON M(Map).
func jsonToBsonM(theJSON []byte) (bson.M, error) {
	var result bson.M

	return result, json.Unmarshal(theJSON, &result)
}

// jsonToBsonD converts raw JSON into binary JSON D(Slice).
func jsonToBsonD(theJSON []byte) (bson.D, error) {
	var result bson.D

	return result, json.Unmarshal(theJSON, &result)
}
