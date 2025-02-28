package engine

import (
	"fmt"

	"github.com/IllidanTwister/ramsql/engine/parser"
	"github.com/IllidanTwister/ramsql/engine/protocol"
)

func dropExecutor(e *Engine, dropDecl *parser.Decl, conn protocol.EngineConn) error {

	// Should have table token
	if dropDecl.Decl == nil ||
		len(dropDecl.Decl) != 1 ||
		dropDecl.Decl[0].Token != parser.TableToken ||
		len(dropDecl.Decl[0].Decl) != 1 {
		return fmt.Errorf("unexpected drop arguments")
	}

	if dropDecl.Decl[0].Decl[0].Token == parser.StarToken {
		//drop all table
		e.dropAll()
	} else {
		table := dropDecl.Decl[0].Decl[0].Lexeme
		r := e.relation(table)
		if r == nil {
			return fmt.Errorf("relation '%s' not found", table)
		}
		e.drop(table)
	}

	return conn.WriteResult(0, 1)
}
