package main

import (
	"database/sql"

	_ "github.com/lib/pq"
	"honnef.co/go/spew"
	"honnef.co/go/tracer"
	"honnef.co/go/tracer/storage/postgres"
)

func main() {
	db, err := sql.Open("postgres", "user=tracer dbname=postgres password=tracer sslmode=disable")
	if err != nil {
		panic(err)
	}
	storage := postgres.New(db)
	spew.Dump(storage.QueryTraces(
		tracer.Query{
			//MaxDuration: time.Second,
			//StartTime: time.Now().Add(-1 * time.Hour),
			AndTags: []tracer.QueryTag{
				{"url", "/hello2", true},
			},
		}))
}
