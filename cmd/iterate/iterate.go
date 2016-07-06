package main

import (
	"database/sql"

	"github.com/tracer/tracer"
	"github.com/tracer/tracer/storage/postgres"

	_ "github.com/lib/pq"
	"honnef.co/go/spew"
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
