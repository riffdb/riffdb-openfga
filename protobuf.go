package riffdbopenfga

import (
	"bytes"
	"errors"

	"google.golang.org/protobuf/proto"
)

var deterministicProto = proto.MarshalOptions{Deterministic: true}

func marshalProto(message proto.Message, maximum int) ([]byte, error) {
	encoded, err := deterministicProto.Marshal(message)
	if err != nil || len(encoded) > maximum {
		return nil, errors.New("riffdb-openfga: protobuf payload exceeds the compiled profile")
	}
	return encoded, nil
}

func unmarshalCanonical(encoded []byte, message proto.Message, maximum int) error {
	if len(encoded) > maximum {
		return errors.New("riffdb-openfga: stored protobuf payload exceeds the compiled profile")
	}
	if err := proto.Unmarshal(encoded, message); err != nil {
		return errors.New("riffdb-openfga: stored protobuf payload is invalid")
	}
	canonical, err := deterministicProto.Marshal(message)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return errors.New("riffdb-openfga: stored protobuf payload is non-canonical")
	}
	return nil
}
