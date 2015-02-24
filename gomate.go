package gomate

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/garyburd/redigo/redis"
)

type Item struct {
	Kind string                 `json:"kind"`
	Id   string                 `json:"id"`
	Term string                 `json:"term"`
	Rank int64                  `json:"rank"`
	Data map[string]interface{} `json:"data"`
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

func Load(kind string, conn redis.Conn) {
	item_base := base(kind)
	phrases, err := redis.Strings(conn.Do("SMEMBERS", item_base))
	if err != nil {
		panic(err)
	}
	for _, p := range phrases {
		conn.Do("DEL", item_base+":"+p)
	}
	conn.Do("DEL", item_base)
	conn.Do("DEL", database(kind))
	conn.Do("DEL", cachebase(kind))

	fmt.Printf("Loading items of type \"%s\"...\n", kind)

	// Start reading input from stdin
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Split(bufio.ScanLines)
	item := Item{Kind: kind}
	i := 0
	for ; scanner.Scan(); i++ {
		raw := scanner.Bytes()
		if err := json.Unmarshal(raw, &item); err != nil {
			panic(err)
		}

		conn.Do("HSET", database(kind), item.Id, raw)
		for _, p := range prefixesForPhrase(item.Term) {
			conn.Do("SADD", item_base, p)
			conn.Do("ZADD", item_base+":"+p, item.Rank, item.Id)
		}
	}

	fmt.Println("Loaded a total of", i, "items.")
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
		exists, _ := conn.Do("EXISTS", cachekey)
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
