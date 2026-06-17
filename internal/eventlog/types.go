// Package eventlog provides append-only event storage and codec.
package eventlog

type Error string

func (e Error) Error() string { return string(e) }

func NewError(msg string) Error { return Error(msg) }
