package gomate

import (
	"github.com/garyburd/redigo/redis"
	"os"
	"path/filepath"
	"testing"
)

func Test_load_values_and_query(t *testing.T) {
	wdir, _ := os.Getwd()
	samples_path := filepath.Join(wdir, "samples", "suburbs.json")
	file, err := os.Open(samples_path)
	if err != nil {
		t.Fatal("Failed to load", samples_path)
	}

	conn, err := Connect("redis://127.0.0.1:9999/5")
	if err != nil {
		t.Fatal("Failed to connect to Redis using redis://localhost:9999/5.")
	}
	conn.Do("FLUSHDB")

	items_loaded, err := BulkLoad("suburb", file, conn)
	if items_loaded != 3 {
		t.Error("Expected to get 3 items loaded but got", items_loaded)
	}

	results := Query("suburb", "wel", conn)
	if len(results) != 2 {
		t.Error("Expected to get 2 matches for \"wel\" but got", len(results))
	}
	if results[0].Term != "Wellington" {
		t.Error("Expected get Wellington as top match for \"wel\" but got", results[0].Term)
	}

	Cleanup("suburb", conn)
	gomate_keys, _ := redis.Values(conn.Do("KEYS", "gomate-*"))
	if len(gomate_keys) != 0 {
		t.Error("Expected 0 gomate keys after cleanup, got", len(gomate_keys))
	}
}

func Test_remove_item(t *testing.T) {
	wdir, _ := os.Getwd()
	samples_path := filepath.Join(wdir, "samples", "suburbs.json")
	file, err := os.Open(samples_path)
	if err != nil {
		t.Fatal("Failed to load", samples_path)
	}

	conn, err := Connect("redis://127.0.0.1:9999/5")
	if err != nil {
		t.Fatal("Failed to connect to Redis using redis://localhost:9999/5.")
	}
	conn.Do("FLUSHDB")

	BulkLoad("suburb", file, conn)

	results := Query("suburb", "keri", conn)
	if len(results) != 1 {
		t.Error("Expected to get 1 match for \"keri\" but got", len(results))
	}

	removed, _ := Remove("suburb", "keri-keri", conn)
	if !removed {
		t.Error("Expected Remove to succeed, got")
	}

	results = Query("suburb", "keri", conn)
	if len(results) != 0 {
		t.Error("Expected to get no matches for \"keri\" but got", len(results))
	}
}
