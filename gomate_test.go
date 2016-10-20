package gomate

import (
	"strings"
	"testing"
)

func TestConnectReturnsErrorWithMessageOnFail(t *testing.T) {
	redis_url := "BAD URL"
	_, err := Connect(redis_url)
	expected_err_msg := "Can't connect to Redis using " + redis_url

	if !strings.HasPrefix(err.Error(), expected_err_msg) {
		t.Error("Expected \""+expected_err_msg+"\" when failing to connect to Redis using bad url, got: ", err)
	}
}

func TestConnectReturnsNoErrorOnSuccess(t *testing.T) {
	redis_url := "redis://localhost:9999/7"
	_, err := Connect(redis_url)

	if err != nil {
		t.Error("Expected no error, got ", err)
	}
}
