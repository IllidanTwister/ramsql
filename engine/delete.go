package engine

import (
	// "errors"
	"fmt"

	"github.com/IllidanTwister/ramsql/engine/log"
	"github.com/IllidanTwister/ramsql/engine/parser"
	"github.com/IllidanTwister/ramsql/engine/protocol"
)

func deleteExecutor(e *Engine, deleteDecl *parser.Decl, conn protocol.EngineConn) error {
	log.Debug("deleteExecutor")

	// get tables to be deleted
	tables := fromExecutor(deleteDecl.Decl[0])

	// If len is 1, it means no predicates so truncate table
	if len(deleteDecl.Decl) == 1 {
		return truncateTable(e, tables[0], conn)
	}

	// get WHERE declaration
	predicate, err := whereExecutor2(e, deleteDecl.Decl[1].Decl, tables[0].name)
	if err != nil {
		return err
	}

	// and delete
	return deleteRows(e, tables, conn, predicate)
}

func deleteRows(e *Engine, tables []*Table, conn protocol.EngineConn, predicate PredicateLinker) error {
	var rowsDeleted int64

	r := e.relation(tables[0].name)
	if r == nil {
		return fmt.Errorf("Table %s not found", tables[0].name)
	}
	r.Lock()
	defer r.Unlock()

	lenRows := len(r.rows)
	for i := 0; i < lenRows; i++ {
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
			switch i {
			case 0:
				r.rows = r.rows[1:]
			case lenRows - 1:
				r.rows = r.rows[:lenRows-1]
			default:
				r.rows = append(r.rows[:i], r.rows[i+1:]...)
				i--
			}
			lenRows--
			rowsDeleted++
		}
	}

	return conn.WriteResult(0, rowsDeleted)
}
