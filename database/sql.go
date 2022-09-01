package database

import (
	"bufio"
	"context"
	"errors"
	"io"
	"strings"

	gactors "github.com/blong14/gache/internal/actors"
)

type parseContext struct {
	scanner    *bufio.Scanner
	query      *gactors.Query
	tkn        string
	evaluators map[string]func(s *bufio.Scanner, q *gactors.Query) error
}

func newParseContext(scanner *bufio.Scanner, query *gactors.Query) *parseContext {
	return &parseContext{
		scanner: scanner,
		query:   query,
		evaluators: map[string]func(s *bufio.Scanner, q *gactors.Query) error{
			"create": func(scanner *bufio.Scanner, query *gactors.Query) error {
				query.Header.Inst = gactors.AddTable
				return nil
			},
			"from": func(scanner *bufio.Scanner, query *gactors.Query) error {
				if scanner.Scan() {
					table := strings.TrimSpace(scanner.Text())
					if strings.HasSuffix(table, ";") {
						query.Header.TableName = []byte(strings.TrimSuffix(table, ";"))
						return nil
					}
					query.Header.TableName = []byte(table)
				}
				return nil
			},
			"insert": func(scanner *bufio.Scanner, query *gactors.Query) error {
				query.Header.Inst = gactors.SetValue
				return nil
			},
			"key": func(scanner *bufio.Scanner, query *gactors.Query) error {
				for scanner.Scan() {
					switch scanner.Text() {
					case "=":
						continue
					default:
						key := strings.TrimSpace(scanner.Text())
						if strings.HasSuffix(key, ";") {
							query.Key = []byte(strings.TrimSuffix(key, ";"))
							return nil
						}
						query.Key = []byte(strings.TrimSuffix(key, ","))
					}
					break
				}
				return nil
			},
			"select": func(scanner *bufio.Scanner, query *gactors.Query) error {
				query.Header.Inst = gactors.GetValue
				if !scanner.Scan() {
					return errors.New("missing token")
				}
				return nil
			},
			"into": func(scanner *bufio.Scanner, query *gactors.Query) error {
				if scanner.Scan() {
					table := strings.TrimSpace(scanner.Text())
					if strings.HasSuffix(table, ";") {
						query.Header.TableName = []byte(strings.TrimSuffix(table, ";"))
						return nil
					}
					query.Header.TableName = []byte(table)
					return nil
				}
				return errors.New("missing table")
			},
			"table": func(scanner *bufio.Scanner, query *gactors.Query) error {
				if scanner.Scan() {
					table := strings.TrimSpace(scanner.Text())
					if strings.HasSuffix(table, ";") {
						query.Header.TableName = []byte(strings.TrimSuffix(table, ";"))
						return nil
					}
				}
				return errors.New("missing table")
			},
			"value": func(scanner *bufio.Scanner, query *gactors.Query) error {
				for scanner.Scan() {
					switch scanner.Text() {
					case "=":
						continue
					default:
						value := strings.TrimSpace(scanner.Text())
						if strings.HasSuffix(value, ";") {
							query.Value = []byte(strings.TrimSuffix(value, ";"))
							return nil
						}
						query.Value = []byte(value)
					}
					break
				}
				return nil
			},
		},
	}
}

func (p *parseContext) SetToken(tkn string) {
	p.tkn = tkn
}

func (p *parseContext) Evaluate() error {
	if eval, ok := p.evaluators[p.tkn]; ok {
		return eval(p.scanner, p.query)
	}
	return nil
}

func parse(src io.Reader) (*gactors.Query, error) {
	scanner := bufio.NewScanner(src)
	scanner.Split(bufio.ScanWords)
	query := gactors.NewQuery(context.Background(), nil)
	ctx := newParseContext(scanner, query)
	for scanner.Scan() {
		ctx.SetToken(scanner.Text())
		if err := ctx.Evaluate(); err != nil {
			return nil, err
		}
	}
	return query, nil
}
