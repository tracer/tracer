package main

import (
	"database/sql"
	"log"
	"net"

	"github.com/tracer/tracer/pb"
	"github.com/tracer/tracer/server"
	"github.com/tracer/tracer/storage/postgres"

	_ "github.com/lib/pq"
	"google.golang.org/grpc"
)

func main() {
	db, err := sql.Open("postgres", "user=tracer dbname=postgres password=tracer sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	storage := postgres.New(db)
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
