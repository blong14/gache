package database

import (
	"bufio"
	"context"
	"errors"
	"io"
	"strings"

	gdb "github.com/blong14/gache/internal/db"
)

type parseContext struct {
	scanner    *bufio.Scanner
	query      *gdb.Query
	tkn        string
	evaluators map[string]func(s *bufio.Scanner, q *gdb.Query) error
}

func newParseContext(scanner *bufio.Scanner, query *gdb.Query) *parseContext {
	return &parseContext{
		scanner: scanner,
		query:   query,
		evaluators: map[string]func(s *bufio.Scanner, q *gdb.Query) error{
			"copy": func(scanner *bufio.Scanner, query *gdb.Query) error {
				query.Header.Inst = gdb.Load
				for scanner.Scan() {
					switch scanner.Text() {
					case "from":
						if scanner.Scan() {
							file := strings.TrimSpace(scanner.Text())
							if strings.HasSuffix(file, ";") {
								query.Header.FileName = []byte(strings.TrimSuffix(file, ";"))
								return nil
							}
							query.Header.FileName = []byte(file)
						}
						return nil
					default:
						table := strings.TrimSpace(scanner.Text())
						query.Header.TableName = []byte(table)
					}
				}
				return nil
			},
			"create": func(scanner *bufio.Scanner, query *gdb.Query) error {
				query.Header.Inst = gdb.AddTable
				return nil
			},
			"from": func(scanner *bufio.Scanner, query *gdb.Query) error {
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
			"insert": func(scanner *bufio.Scanner, query *gdb.Query) error {
				query.Header.Inst = gdb.SetValue
				return nil
			},
			"key": func(scanner *bufio.Scanner, query *gdb.Query) error {
				for scanner.Scan() {
					switch scanner.Text() {
					case "=":
						if query.Header.Inst == gdb.GetRange {
							query.Header.Inst = gdb.GetValue
						}
						continue
					case "and":
						if scanner.Scan() {
							query.KeyRange.End = []byte(strings.TrimSuffix(scanner.Text(), ";"))
						}
					case "between":
						if scanner.Scan() {
							query.KeyRange.Start = []byte(scanner.Text())
						}
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
			"select": func(scanner *bufio.Scanner, query *gdb.Query) error {
				query.Header.Inst = gdb.GetRange
				if !scanner.Scan() {
					return errors.New("missing token")
				}
				return nil
			},
			"into": func(scanner *bufio.Scanner, query *gdb.Query) error {
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
			"table": func(scanner *bufio.Scanner, query *gdb.Query) error {
				if scanner.Scan() {
					table := strings.TrimSpace(scanner.Text())
					if strings.HasSuffix(table, ";") {
						query.Header.TableName = []byte(strings.TrimSuffix(table, ";"))
						return nil
					}
				}
				return errors.New("missing table")
			},
			"value": func(scanner *bufio.Scanner, query *gdb.Query) error {
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

func parse(src io.Reader) (*gdb.Query, error) {
	scanner := bufio.NewScanner(src)
	scanner.Split(bufio.ScanWords)
	query := gdb.NewQuery(context.Background(), nil)
	ctx := newParseContext(scanner, query)
	for scanner.Scan() {
		ctx.SetToken(scanner.Text())
		if err := ctx.Evaluate(); err != nil {
			return nil, err
		}
	}
	return query, nil
}
