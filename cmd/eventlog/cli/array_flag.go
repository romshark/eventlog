package cli

import "fmt"

type arrayFlag []string

func (f *arrayFlag) String() string {
	return fmt.Sprintf("%v", *f)
}

func (f *arrayFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}
