package engine

import (
	"fmt"
	"github.com/IllidanTwister/ramsql/engine/log"
	"strconv"
	"strings"
	"time"

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
func setExecutor(setDecl *parser.Decl) (map[string]*parser.Decl, error) {

	values := make(map[string]*parser.Decl)

	for _, attr := range setDecl.Decl {
		if attr.Decl[0].Token != parser.EqualityToken {
			return nil, fmt.Errorf("setExecutor not Equality after arrtibute")
		}
		values[attr.Lexeme] = attr.Decl[1]
	}

	return values, nil
}

func updateValues(r *Relation, row int, values map[string]*parser.Decl) error {
	attributeMap := make(map[string]interface{})
	for i, attribute := range r.table.attributes {
		attributeMap[attribute.name] = r.rows[row].Values[i]
	}

	for i := range r.table.attributes {
		val, ok := values[r.table.attributes[i].name]
		if !ok {
			continue
		}
		log.Debug("Type of '%s' is '%s'\n", r.table.attributes[i].name, r.table.attributes[i].typeName)
		var result interface{}
		var err error
		if val.Token == parser.AttributeToken {
			result, err = getAttributeValue(val, attributeMap)
			if err != nil {
				return err
			}
		} else {
			switch strings.ToLower(r.table.attributes[i].typeName) {
			case "timestamp", "localtimestamp":
				if val.Lexeme == "current_timestamp" || val.Lexeme == "now()" {
					result = time.Now().Format(parser.DateLongFormat)
				}
			default:
				result = val.Lexeme
			}
		}
		r.rows[row].Values[i] = fmt.Sprintf("%v", result)
	}

	return nil
}

func getAttributeValue(val *parser.Decl, attributeMap map[string]interface{}) (interface{}, error) {
	var result string
	if val.Token == parser.AttributeToken {
		if attributeValue, ok := attributeMap[val.Lexeme]; !ok {
			return nil, fmt.Errorf("setAttributeValue error, no attribute of %s find", val.Lexeme)
		} else {
			result = attributeValue.(string)
		}
	} else {
		result = val.Lexeme
	}
	if val.Decl != nil && len(val.Decl) > 1 {
		nextValue, err := getAttributeValue(val.Decl[1], attributeMap)
		if err != nil {
			return nil, err
		}
		nextValueStr := nextValue.(string)
		resultInt, err1 := strconv.Atoi(result)
		nextValueInt, err2 := strconv.Atoi(nextValueStr)
		switch val.Decl[0].Token {
		case parser.AddToken:
			if err1 == nil && err2 == nil {
				result = strconv.Itoa(resultInt + nextValueInt)
			} else {
				result = result + nextValueStr
			}
		case parser.MinusToken:
			if err1 == nil && err2 == nil {
				result = strconv.Itoa(resultInt - nextValueInt)
			} else {
				return nil, fmt.Errorf("operation '-' between strings")
			}
		default:
			return nil, fmt.Errorf("unkonwn operation Token %v", val.Decl[1].Token)
		}
	}
	return result, nil
}
