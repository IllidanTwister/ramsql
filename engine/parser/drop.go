package parser

import (
	"github.com/IllidanTwister/ramsql/engine/log"
)

func (p *parser) parseDrop() (*Instruction, error) {
	i := &Instruction{}

	trDecl, err := p.consumeToken(DropToken)
	if err != nil {
		log.Debug("WTF\n")
		return nil, err
	}
	i.Decls = append(i.Decls, trDecl)

	tableDecl, err := p.consumeToken(TableToken)
	if err != nil {
		log.Debug("Consume table !\n")
		return nil, err
	}
	trDecl.Add(tableDecl)

	//support 'drop table *' for test clear
	if p.is(StarToken) {
		starDecl, err := p.consumeToken(StarToken)
		if err != nil {
			return nil, err
		}
		tableDecl.Add(starDecl)
	} else {
		// Should be a table name
		nameDecl, err := p.parseQuotedToken()
		if err != nil {
			log.Debug("UH ?\n")
			return nil, err
		}
		tableDecl.Add(nameDecl)
	}
	return i, nil
}
