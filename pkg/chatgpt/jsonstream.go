package chatgpt

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
)

type jsonStreamDecoder interface {
	decodeJSONStream(dec *json.Decoder) error
}

func unexpectedJSONToken(dec *json.Decoder, tok json.Token, want reflect.Type) error {
	return &json.UnmarshalTypeError{Value: fmt.Sprintf("%T", tok), Type: want, Offset: dec.InputOffset()}
}

func unexpectedEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

func decodeJSONObject(dec *json.Decoder, field func(key string) error) error {
	tok, err := dec.Token()
	if err != nil || tok == nil {
		return err
	}
	if tok != json.Delim('{') {
		return unexpectedJSONToken(dec, tok, reflect.TypeFor[map[string]any]())
	}
	for dec.More() {
		tok, err = dec.Token()
		if err != nil {
			return unexpectedEOF(err)
		}
		key, ok := tok.(string)
		if !ok {
			return unexpectedJSONToken(dec, tok, reflect.TypeFor[string]())
		}
		if err = field(key); err != nil {
			return unexpectedEOF(err)
		}
	}
	_, err = dec.Token()
	return unexpectedEOF(err)
}

func decodeJSONArray(dec *json.Decoder, element func() error) error {
	tok, err := dec.Token()
	if err != nil || tok == nil {
		return err
	}
	if tok != json.Delim('[') {
		return unexpectedJSONToken(dec, tok, reflect.TypeFor[[]any]())
	}
	for dec.More() {
		if err = element(); err != nil {
			return unexpectedEOF(err)
		}
	}
	_, err = dec.Token()
	return unexpectedEOF(err)
}

func skipJSONValue(dec *json.Decoder) error {
	var skip json.RawMessage
	return dec.Decode(&skip)
}
