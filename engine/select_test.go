package engine_test

import (
	"database/sql"
	"testing"

	"github.com/IllidanTwister/ramsql/engine/log"
)

func TestSelectNoOp(t *testing.T) {
	log.UseTestLogger(t)
	db, err := sql.Open("ramsql", "TestSelectNoOp")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			t.Fatalf("sql.Close : Error : %s\n", err)
		}
	}()

	batch := []string{
		`CREATE TABLE account (id BIGSERIAL, email TEXT)`,
		`INSERT INTO account (email) VALUES ("foo@bar.com")`,
		`INSERT INTO account (email) VALUES ("bar@bar.com")`,
		`INSERT INTO account (email) VALUES ("foobar@bar.com")`,
		`INSERT INTO account (email) VALUES ("babar@bar.com")`,
	}

	for _, b := range batch {
		_, err = db.Exec(b)
		if err != nil {
			t.Fatalf("sql.Exec: Error: %s\n", err)
		}
	}

	query := `SELECT * from account WHERE 1 = 1`
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("cannot create table: %s", err)
	}

	nb := 0
	for rows.Next() {
		nb++
	}

	if nb != 4 {
		t.Fatalf("Expected 4 rows, got %d", nb)
	}

}

func TestSelect(t *testing.T) {
	log.UseTestLogger(t)
	db, err := sql.Open("ramsql", "TestSelect")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			t.Fatalf("sql.Close : Error : %s\n", err)
		}
	}()

	_, err = db.Exec("CREATE TABLE account (id INT, email TEXT)")
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	_, err = db.Exec("INSERT INTO account ('id', 'email') VALUES (2, 'bar@bar.com')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	_, err = db.Exec("INSERT INTO account ('id', 'email') VALUES (1, 'foo@bar.com')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	_, err = db.Query("SELECT * FROM account WHERE email = $1", "foo@bar.com")
	if err != nil {
		t.Fatalf("sql.Query error : %s", err)
	}

	rows, err := db.Query("SELECT * FROM account WHERE (email = $1)", "foo@bar.com")
	if err != nil {
		t.Fatalf("sql.Query error : %s", err)
	}

	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("rows.Column : %s", err)
		return
	}

	if len(columns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(columns))
	}

	row := db.QueryRow("SELECT * FROM account WHERE email = $1", "foo@bar.com")
	if row == nil {
		t.Fatalf("sql.QueryRow error")
	}

	var email string
	var id int
	err = row.Scan(&id, &email)
	if err != nil {
		t.Fatalf("row.Scan: %s", err)
	}

	if id != 1 {
		t.Fatalf("Expected id = 1, got %d", id)
	}

	if email != "foo@bar.com" {
		t.Fatalf("Expected email = <foo@bar.com>, got <%s>", email)
	}
}

func TestSelectBool(t *testing.T) {
	log.UseTestLogger(t)
	db, err := sql.Open("ramsql", "TestSelectBool")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			t.Fatalf("sql.Close : Error : %s\n", err)
		}
	}()

	_, err = db.Exec("CREATE TABLE account (id INT, email TEXT, deleted bool)")
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	_, err = db.Exec("INSERT INTO account ('id', 'email', 'deleted') VALUES (2, 'bar@bar.com', true)")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	_, err = db.Exec("INSERT INTO account ('id', 'email', 'deleted') VALUES (1, 'foo@bar.com', false)")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	_, err = db.Query("SELECT * FROM account WHERE deleted = $1", "true")
	if err != nil {
		t.Fatalf("sql.Query error : %s", err)
	}

	rows, err := db.Query("SELECT * FROM account WHERE (deleted = $1)", "false")
	if err != nil {
		t.Fatalf("sql.Query error : %s", err)
	}
	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("rows.Column : %s", err)
		return
	}
	if len(columns) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(columns))
	}

	row := db.QueryRow("SELECT * FROM account WHERE (deleted = $1)", 0)
	if row == nil {
		t.Fatalf("sql.QueryRow error")
	}

	var email string
	var id int
	var deleted bool
	err = row.Scan(&id, &email, &deleted)
	if err != nil {
		t.Fatalf("row.Scan: %s", err)
	}

	if id != 1 {
		t.Fatalf("Expected id = 1, got %d", id)
	}

	if email != "foo@bar.com" {
		t.Fatalf("Expected email = <foo@bar.com>, got <%s>", email)
	}

	if deleted {
		t.Fatalf("Expected deleted = false, got <%v>", deleted)
	}
}

func TestCount(t *testing.T) {
	log.UseTestLogger(t)
	db, err := sql.Open("ramsql", "TestCount")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			t.Fatalf("sql.Close : Error : %s\n", err)
		}
	}()

	batch := []string{
		`CREATE TABLE account (id BIGSERIAL, email TEXT)`,
		`INSERT INTO account (email) VALUES ("foo@bar.com")`,
		`INSERT INTO account (email) VALUES ("bar@bar.com")`,
		`INSERT INTO account (email) VALUES ("foobar@bar.com")`,
		`INSERT INTO account (email) VALUES ("babar@bar.com")`,
	}

	for _, b := range batch {
		_, err = db.Exec(b)
		if err != nil {
			t.Fatalf("sql.Exec: Error: %s\n", err)
		}
	}

	var count int64
	err = db.QueryRow(`SELECT COUNT(*) FROM account WHERE 1=1`).Scan(&count)
	if err != nil {
		t.Fatalf("cannot select COUNT of account: %s\n", err)
	}

	if count != 4 {
		t.Fatalf("Expected count to be 4, not %d", count)
	}

	err = db.QueryRow(`SELECT COUNT(i_dont_exist_lol) FROM account WHERE 1=1`).Scan(&count)
	if err == nil {
		t.Fatalf("Expected an error from a non existing attribute")
	}
}
