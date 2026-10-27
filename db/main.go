package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "modernc.org/sqlite"
)

// sql quesry for creating tabel
var schema = `
CREATE TABLE IF NOT EXISTS users (
id INTEGER PRIMARY KEY AUTOINCREMENT,
name TEXT NOT NULL,
email TEXT NOT NULL UNIQUE,
hashed_password BLOB NOT NULL, -- Storing as BLOB for byte slice
created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
)`

func main() {
	db_name := "users.db"
	db, err := sql.Open("sqlite", db_name)
	if err != nil {
		log.Fatal("failed to open database", err)
	}
	defer db.Close()
	// ping to db
	err = db.Ping()
	if err != nil {
		log.Fatal("failed to ping database", err)
	}
	fmt.Println("DB ping successful ")
	// execute schema query
	_, err = db.Exec(schema)
	if err != nil {
		log.Fatal("failed to execute schema query", err)
	}

}
