package internal

import (
	"fmt"
	"strings"
)

type MultiError struct {
	Errors []error
}

func (e *MultiError) Append(err error) {
	e.Errors = append(e.Errors, err)
}

func (e *MultiError) Error() string {
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

func (e *MultiError) ErrorOrNil() error {
	if e == nil {
		return nil
	}
	if len(e.Errors) == 0 {
		return nil
	}

	return e
}
