package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "modernc.org/sqlite"
)

// sql quesry for creating tabel
var schema = `
CREATE TABLE IF NOT EXISTS users (
id INTEGER PRIMARY KEY AUTOINCREMENT,
name TEXT NOT NULL,
email TEXT NOT NULL UNIQUE,
hashed_password TEXT NOT NULL, -- Storing as BLOB for byte slice
created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
)`

type User struct {
	ID             int64     `json:"id"`
	Name           string    `json:"name"`
	Email          string    `json:"email"`
	HashedPassword []byte    `json:"hashed_password"`
	CreatedAt      time.Time `json:"created_at"`
}

func main() {
	db_name := "usersDB.db"
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

func createUser(db *sql.DB, name, email, hashpassword string) {
	stmnt := `INSERT INTO users (name, email, hashed_password) VALUES (?,?,?);`

	result, err := db.Exec(stmnt, name, email, hashpassword)
	if err != nil {
		log.Fatal("failed to create user", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		log.Fatal("failed to get last insert id", err)
	}
	fmt.Println("user created with id", id)
}

func getUserByEmail(db *sql.DB, email string) (*User, error) {
	stmnt := `SELECT id, name, email, hashed_password, created_at FROM users WHERE email = ?;`
	row := db.QueryRow(stmnt, email)
	var user User
	err := row.Scan(&user.ID, &user.Name, &user.Email, &user.HashedPassword, &user.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &user, nil
}

func getAllUsers(db *sql.DB) ([]User, error) {
	stmnt := `SELECT id, name, email, hashed_password, created_at FROM users;`
	rows, err := db.Query(stmnt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var users []User
	for rows.Next() {
		var user User
		err := rows.Scan(&user.ID, &user.Name, &user.Email, &user.HashedPassword, &user.CreatedAt)
		if err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	return users, nil
}
