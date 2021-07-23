package engine

import (
	"fmt"
	"strings"
	"time"

	"github.com/IllidanTwister/ramsql/engine/log"
	"github.com/IllidanTwister/ramsql/engine/parser"
	"github.com/IllidanTwister/ramsql/engine/protocol"
)

/*
|-> update
	|-> account
	|-> set
	      |-> email
					|-> =
					|-> roger@gmail.com
  |-> where
        |-> id
					|-> =
					|-> 2
*/
func updateExecutor(e *Engine, updateDecl *parser.Decl, conn protocol.EngineConn) error {
	var num int64

	updateDecl.Stringy(0)

	// Fetch table from name and write lock it
	r := e.relation(updateDecl.Decl[0].Lexeme)
	if r == nil {
		return fmt.Errorf("Table %s does not exists", updateDecl.Decl[0].Lexeme)
	}
	r.Lock()
	r.Unlock()

	// Set decl
	values, err := setExecutor(updateDecl.Decl[1])
	if err != nil {
		return err
	}

	// Where decl
	predicate, err := whereExecutor2(e, updateDecl.Decl[2].Decl, r.table.name)
	if err != nil {
		return err
	}

	for i := range r.rows {
		// create virtualrow
		row := make(virtualRow)
		for index := range r.rows[i].Values {
			v := Value{
				v:      r.rows[i].Values[index],
				valid:  true,
				lexeme: r.table.attributes[index].name,
				table:  r.table.name,
			}
			row[v.table+"."+v.lexeme] = v
		}
		// If the row validate all predicates, write it
		res, err := predicate.Eval(row)
		if err != nil {
			return err
		}

		if res {
			num++
			err = updateValues(r, i, values)
			if err != nil {
				return err
			}
		}
	}

	return conn.WriteResult(0, num)
}

/*
	|-> set
	      |-> email
					|-> =
					|-> roger@gmail.com
*/
func setExecutor(setDecl *parser.Decl) (map[string]interface{}, error) {

	values := make(map[string]interface{})

	for _, attr := range setDecl.Decl {
		values[attr.Lexeme] = attr.Decl[1].Lexeme
	}

	return values, nil
}

func updateValues(r *Relation, row int, values map[string]interface{}) error {
	for i := range r.table.attributes {
		val, ok := values[r.table.attributes[i].name]
		if !ok {
			continue
		}
		log.Debug("Type of '%s' is '%s'\n", r.table.attributes[i].name, r.table.attributes[i].typeName)
		switch strings.ToLower(r.table.attributes[i].typeName) {
		case "timestamp", "localtimestamp":
			s, ok := val.(string)
			if ok && (s == "current_timestamp" || s == "now()") {
				val = time.Now()
			}
			// format time.Time into parsable string
			if t, ok := val.(time.Time); ok {
				val = t.Format(parser.DateLongFormat)
			}
		}
		r.rows[row].Values[i] = fmt.Sprintf("%v", val)
	}

	return nil
}
