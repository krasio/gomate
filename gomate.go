package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/codegangsta/cli"
	"github.com/garyburd/redigo/redis"
	"github.com/soveran/redisurl"
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

func main() {
	app := cli.NewApp()
	app.Name = "gomate"
	app.Usage = "autocompelate like a boss"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "redis, r",
			Value:  "redis://localhost:6379/0",
			Usage:  "Redis connection string",
			EnvVar: "GOMATE_REDIS_URL",
		},
	}

	app.Commands = []cli.Command{
		{
			Name:        "load",
			ShortName:   "l",
			Usage:       "Replaces collection specified by TYPE with items read from stdin in the JSON lines format.",
			Description: "load [TYPE] < path/to/data.json",
			Action: func(c *cli.Context) {
				kind := c.Args()[0]
				// Connect to Redis
				fmt.Printf("Using %s.\n", c.GlobalString("redis"))
				conn, err := redisurl.ConnectToURL(c.GlobalString("redis"))
				if err != nil {
					panic(err)
				}
				defer conn.Close()

				item_base := base(kind)
				phrases, err := redis.Strings(conn.Do("SMEMBERS", item_base))
				for _, p := range phrases {
					conn.Do("DEL", item_base+":"+p)
				}
				conn.Do("DEL", item_base)
				conn.Do("DEL", database(kind))
				conn.Do("DEL", cachebase(kind))

				fmt.Printf("Loading items of type \"%s\"...\n", c.Args()[0])

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
			},
		},
		{
			Name:        "query",
			ShortName:   "q",
			Usage:       "Queries for items from collection specified by TYPE.",
			Description: "query [TYPE] [TERM]",
			Action: func(c *cli.Context) {
				kind := c.Args()[0]
				query := c.Args()[1]
				// Connect to Redis
				fmt.Printf("Using %s.\n", c.GlobalString("redis"))
				conn, err := redisurl.ConnectToURL(c.GlobalString("redis"))
				if err != nil {
					panic(err)
				}
				fmt.Printf("Query %s for \"%s\":\n", kind, query)

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

				for _, match := range matches {
					fmt.Printf("  %s\n", match.Term)
				}
			},
		},
	}

	app.Run(os.Args)
}
