package engine_test

import (
	"database/sql"
	"testing"

	"github.com/IllidanTwister/ramsql/engine/log"

	_ "github.com/IllidanTwister/ramsql/driver"
)

func TestDrop(t *testing.T) {
	log.UseTestLogger(t)
	db, err := sql.Open("ramsql", "TestDrop")
	if err != nil {
		t.Fatalf("%s", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE account (id INT, email TEXT)")
	if err != nil {
		t.Fatalf("%s", err)
	}

	_, err = db.Exec("DROP TABLE account")
	if err != nil {
		t.Fatalf("cannot drop table: %s", err)
	}
}

func TestDropAll(t *testing.T) {
	log.UseTestLogger(t)
	db, err := sql.Open("ramsql", "TestDropAll")
	if err != nil {
		t.Fatalf("%s", err)
	}
	db.Close()
	db, err = sql.Open("ramsql", "TestDropAll")
	if err != nil {
		t.Fatalf("%s", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE account (id INT, email TEXT)")
	if err != nil {
		t.Fatalf("%s", err)
	}
	_, err = db.Exec("CREATE TABLE account (id INT, email TEXT)")
	if err == nil {
		t.Fatalf("able to CREATE same TABLE: account")
	}

	_, err = db.Exec("CREATE TABLE account2 (id INT, email TEXT)")
	if err != nil {
		t.Fatalf("%s", err)
	}

	_, err = db.Exec("DROP TABLE *")
	if err != nil {
		t.Fatalf("cannot drop table: %s", err)
	}

	_, err = db.Exec("CREATE TABLE account (id INT, email TEXT)")
	if err != nil {
		t.Fatalf("%s", err)
	}
}
