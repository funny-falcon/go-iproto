package iproto_test

import (
	"bytes"
	. "iproto"
	"testing"
)

func TestConnect(t *testing.T) {
	var (
		rid      int32
		response *Response
		err      error
		conn     *IProto
	)

	conn, err = Connect("localhost:33013")
	if err != nil {
		t.Errorf("Error: %s", err.Error())
	}

	rid = 17
	response, err = conn.Request(rid, new(bytes.Buffer))
	if err != nil {
		t.Errorf("Error: %s", err.Error())
	}
	if response.requestType != rid {
		t.Errorf("Error: requestType should be %d, not %d", rid, response.requestType)
	}
	if response.requestID != 1 {
		t.Errorf("Error: requestID should be %d, not %d", 1, response.requestID)
	}

	rid = 20
	response, err = conn.Request(rid, new(bytes.Buffer))
	if err != nil {
		t.Errorf("Error: %s", err.Error())
	}
	if response.requestType != rid {
		t.Errorf("Error: requestType should be %d, not %d", rid, response.requestType)
	}
	if response.requestID != 2 {
		t.Errorf("Error: requestID should be %d, not %d", 2, response.requestID)
	}
}
