package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/tracer/tracer"
	"github.com/tracer/tracer/cmd/tracer/config"
	"github.com/tracer/tracer/pb"
	"github.com/tracer/tracer/server"
	"github.com/tracer/tracer/storage/postgres"

	_ "github.com/lib/pq"
	"google.golang.org/grpc"
)

func loadStorage(conf config.Config) (tracer.Storer, error) {
	engine, err := conf.Storage()
	if err != nil {
		return nil, err
	}
	switch engine {
	case "postgres":
		return loadPostgres(conf)
	default:
		return nil, fmt.Errorf("unsupported storage engine: %s", engine)
	}
}

func loadPostgres(conf config.Config) (tracer.Storer, error) {
	storageConf, err := conf.StorageConfig()
	if err != nil {
		return nil, err
	}
	url, ok := storageConf["url"].(string)
	if !ok {
		return nil, errors.New("missing url for postgres backend")
	}
	db, err := sql.Open("postgres", url)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("drror connecting to PostgreSQL database: %s", err)
	}
	return postgres.New(db), nil
}

func main() {
	f, err := os.Open("example.conf")
	if err != nil {
		log.Fatalln("Couldn't load config:", err)
	}
	conf, err := config.Load(f)
	if err != nil {
		log.Fatalln("Couldn't load config:", err)
	}
	_ = f.Close()

	storage, err := loadStorage(conf)
	if err != nil {
		log.Fatal(err)
	}
	srv := &server.Server{Storer: storage}

	l, err := net.Listen("tcp", ":9999")
	if err != nil {
		log.Fatal(err)
	}
	s := grpc.NewServer()
	pb.RegisterStorerServer(s, srv)
	if err := s.Serve(l); err != nil {
		log.Fatal(err)
	}
}
