package database

import (
	"database/sql"
	_ "github.com/lib/pq"
	"log"
)

func DBConnection() *sql.DB {
	connStr := "host=localhost port=6000 user=admin password=secret dbname=mydatabase sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Failed to connect to the database:", err)
	}

	if err = db.Ping(); err != nil {
		log.Fatal("Unable to reach the database:", err)
	}

	log.Println("Connected to the database successfully!")
	return db
}
