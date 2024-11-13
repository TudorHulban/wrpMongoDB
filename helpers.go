package mongoclient

import (
	"encoding/json"

	"go.mongodb.org/mongo-driver/bson"
)

func jsonToBsonM(jsonRaw []byte) (bson.M, error) {
	var result bson.M

	return result,
		json.Unmarshal(jsonRaw, &result)
}

func jsonToBsonD(jsonRaw []byte) (bson.D, error) {
	var result bson.D

	return result,
		json.Unmarshal(jsonRaw, &result)
}
