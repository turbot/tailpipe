package types

type Database interface {
	Open() error
	Close() error
}

type SchemaInterface interface {
	Identifier() string
}
