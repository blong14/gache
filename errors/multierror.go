package gerrors

import (
	"errors"
	"fmt"
	"strings"
)

type Error struct {
	msg    string
	Errors []error
}

func (e *Error) Error() string {
	if len(e.Errors) == 1 {
		return fmt.Sprintf("1 error occurred:\n\t* %s\n\n", e.Errors[0])
	}
	points := make([]string, len(e.Errors))
	for i, err := range e.Errors {
		points[i] = fmt.Sprintf("* %s", err)
	}
	return fmt.Sprintf(
		"%d errors occurred:\n\t%s\n\n",
		len(e.Errors), strings.Join(points, "\n\t"))
}

func (e *Error) ErrorOrNil() error {
	if e == nil {
		return nil
	}
	if len(e.Errors) == 0 {
		return nil
	}
	return e
}

func (e *Error) GoString() string {
	return fmt.Sprintf("*%#v", *e)
}

func (e *Error) WrappedErrors() []error {
	if e == nil {
		return nil
	}
	return e.Errors
}

func (e *Error) Unwrap() error {
	if e == nil || len(e.Errors) == 0 {
		return nil
	}
	if len(e.Errors) == 1 {
		return e.Errors[0]
	}
	errs := make([]error, len(e.Errors))
	copy(errs, e.Errors)
	return chain(errs)
}

type chain []error

func (e chain) Error() string {
	return e[0].Error()
}

func (e chain) Unwrap() error {
	if len(e) == 1 {
		return nil
	}
	return e[1:]
}

func (e chain) As(target interface{}) bool {
	return errors.As(e[0], target)
}

func (e chain) Is(target error) bool {
	return errors.Is(e[0], target)
}

func Append(err error, errs ...error) *Error {
	switch err := err.(type) {
	case *Error:
		if err == nil {
			err = new(Error)
		}
		for _, e := range errs {
			switch e := e.(type) {
			case *Error:
				if e != nil {
					err.Errors = append(err.Errors, e.Errors...)
				}
			default:
				if e != nil {
					err.Errors = append(err.Errors, e)
				}
			}
		}
		return err
	default:
		newErrs := make([]error, 0, len(errs)+1)
		if err != nil {
			newErrs = append(newErrs, err)
		}
		newErrs = append(newErrs, errs...)
		return Append(&Error{}, newErrs...)
	}
}
