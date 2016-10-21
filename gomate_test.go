package gomate

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/rafaeljusto/redigomock"
)

func TestConnectReturnsErrorWithMessageOnFail(t *testing.T) {
	redis_url := "BAD URL"
	_, err := Connect(redis_url)
	expected_err_msg := "Can't connect to Redis using " + redis_url

	if !strings.HasPrefix(err.Error(), expected_err_msg) {
		t.Errorf("Expected \"%v...\" when failing to connect to Redis using bad url, got: \"%v\"", expected_err_msg, err)
	}
}

func TestConnectReturnsNoErrorOnSuccess(t *testing.T) {
	redis_url := "redis://localhost:9999/7"
	_, err := Connect(redis_url)

	if err != nil {
		t.Errorf("Expected no error, got \"%v\"", err)
	}
}

func TestCleanupReturnsErrorIfCantFetchPhrases(t *testing.T) {
	conn := redigomock.NewConn()
	conn.Command("SMEMBERS", "gomate-index:suburb").ExpectError(fmt.Errorf("Redis fail!"))
	err := Cleanup("suburb", conn)

	expected_err_msg := "Failed to cleanup for suburb: Redis fail!"

	if !strings.HasPrefix(err.Error(), expected_err_msg) {
		t.Errorf("Expected \"%v...\" when failing to connect to Redis using bad url, got: \"%v\"", expected_err_msg, err)
	}
}

func TestBulkLoad_returns_error_if_cant_cleanup(t *testing.T) {
	conn := redigomock.NewConn()
	conn.Command("SMEMBERS", "gomate-index:suburb").ExpectError(fmt.Errorf("Redis fail!"))

	inserted, err := BulkLoad("suburb", os.Stdin, conn)

	if inserted != 0 || err == nil {
		t.Errorf("Expected 0 and error, got ", inserted, err)
	}
}
