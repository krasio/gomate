package gomate

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"

	"github.com/garyburd/redigo/redis"
	"github.com/pkg/errors"
	"github.com/soveran/redisurl"
)

type Item struct {
	Kind string                 `json:"kind"`
	Id   string                 `json:"id"`
	Term string                 `json:"term"`
	Rank int64                  `json:"rank"`
	Data map[string]interface{} `json:"data"`
	Raw  string
}

func Connect(url string) (conn redis.Conn, err error) {
	fmt.Printf("Connecting to Redis using %s.\n", url)
	conn, err = redisurl.ConnectToURL(url)

	return conn, errors.WithMessage(err, "Can't connect to Redis using "+url)
}

func BulkLoad(kind string, reader io.Reader, conn redis.Conn) (int, error) {
	if err := Cleanup(kind, conn); err != nil {
		return 0, err
	}

	scanner := bufio.NewScanner(reader)
	scanner.Split(bufio.ScanLines)

	item := Item{Kind: kind}
	i := 0
	for ; scanner.Scan(); i++ {
		raw := scanner.Bytes()
		if err := json.Unmarshal(raw, &item); err == nil {
			item.Raw = string(raw)
			LoadItem(&item, conn)
		}
	}

	fmt.Println("Loaded a total of", i, "items.")
	return i, nil
}

func LoadItem(item *Item, conn redis.Conn) error {
	err := conn.Send("MULTI")
	if err == nil {
		conn.Send("HSET", database(item.Kind), item.Id, item.Raw)
		item_base := base(item.Kind)
		for _, p := range prefixesForPhrase(item.Term) {
			conn.Send("SADD", item_base, p)
			conn.Send("ZADD", item_base+":"+p, item.Rank, item.Id)
		}
		_, err = conn.Do("EXEC")
	}

	return errors.WithMessage(err, "Failed to load item for "+item.Kind)
}

func Remove(kind string, id string, conn redis.Conn) (bool, error) {
	raw, _ := redis.Bytes(conn.Do("HGET", database(kind), id))
	if raw == nil {
		return false, nil
	}

	item := Item{Kind: kind}
	if err := json.Unmarshal(raw, &item); err != nil {
		return false, errors.Wrap(err, "Failed to decode data for "+kind+" "+id)
	}

	err := conn.Send("MULTI")
	if err == nil {
		conn.Send("HDEL", database(kind), id)
		item_base := base(item.Kind)
		for _, p := range prefixesForPhrase(item.Term) {
			conn.Send("SREM", item_base, p)
			conn.Send("ZREM", item_base+":"+p, item.Id)
			conn.Send("ZREM", cachebase(item.Kind)+":"+p, item.Id)
		}
		_, err = conn.Do("EXEC")
		if err != nil {
			panic(err)
			fmt.Println("ERR " + err.Error())
		}
	}

	return true, nil
}

func Query(kind string, query string, conn redis.Conn) []Item {
	matches := []Item{}
	words := []string{}

	for _, word := range strings.Split(normalize(query), " ") {
		if len(word) > 2 {
			words = append(words, word)
		}
	}
	if len(words) > 0 {
		sort.Strings(words)
		cachekey := cachebase(kind) + ":" + strings.Join(words, "|")
		exists, err := conn.Do("EXISTS", cachekey)
		if err != nil {
			panic(err)
		}
		if exists.(int64) == 0 {
			interkeys := make([]string, len(words))
			for i, word := range words {
				interkeys[i] = base(kind) + ":" + word
			}
			conn.Do("ZINTERSTORE", redis.Args{}.Add(cachekey).Add(len(interkeys)).AddFlat(interkeys)...)
			conn.Do("EXPIRE", cachekey, 10*60)
		}

		ids, _ := redis.Strings(conn.Do("ZREVRANGE", cachekey, 0, 5-1))
		results, _ := redis.Strings(conn.Do("HMGET", redis.Args{}.Add(database(kind)).AddFlat(ids)...))
		for _, r := range results {
			item := Item{Kind: kind}
			if err := json.Unmarshal([]byte(r), &item); err != nil {
				panic(err)
			}
			matches = append(matches, item)
		}
	}

	return matches
}

func Cleanup(kind string, conn redis.Conn) error {
	err_message := "Failed to cleanup for " + kind
	item_base := base(kind)

	phrases, err := redis.Strings(conn.Do("SMEMBERS", item_base))
	if err != nil {
		return errors.Wrap(err, err_message)
	}

	err = conn.Send("MULTI")
	if err == nil {
		for _, p := range phrases {
			conn.Send("DEL", item_base+":"+p)
			conn.Send("DEL", cachebase(kind)+":"+p)
		}
		conn.Send("DEL", item_base)
		conn.Send("DEL", database(kind))
		conn.Send("DEL", cachebase(kind))
		_, err = conn.Do("EXEC")
	}

	return errors.WithMessage(err, err_message)
}

func base(kind string) string {
	return "gomate-index:" + kind
}

func database(kind string) string {
	return "gomate-data:" + kind
}

func cachebase(kind string) string {
	return "gomate-cache:" + kind
}

func prefixesForPhrase(phrase string) []string {
	words := strings.Split(normalize(phrase), " ")
	prefixes := []string{}
	for _, word := range words {
		for i := 2; i <= len(word); i++ {
			prefixes = append(prefixes, word[:i])
		}
	}

	return prefixes
}

func normalize(phrase string) string {
	cleanup := regexp.MustCompile(`[^[:word:] ]`)
	return strings.ToLower(cleanup.ReplaceAllString(phrase, ""))
}
