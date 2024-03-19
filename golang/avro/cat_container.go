// Code generated by github.com/actgardner/gogen-avro/v10. DO NOT EDIT.
/*
 * SOURCES:
 *     Cat.avsc
 *     Breed.avsc
 */
package avro

import (
	"io"

	"github.com/actgardner/gogen-avro/v10/compiler"
	"github.com/actgardner/gogen-avro/v10/container"
	"github.com/actgardner/gogen-avro/v10/vm"
)

func NewCatWriter(writer io.Writer, codec container.Codec, recordsPerBlock int64) (*container.Writer, error) {
	str := NewCat()
	return container.NewWriter(writer, codec, recordsPerBlock, str.Schema())
}

// container reader
type CatReader struct {
	r io.Reader
	p *vm.Program
}

func NewCatReader(r io.Reader) (*CatReader, error) {
	containerReader, err := container.NewReader(r)
	if err != nil {
		return nil, err
	}

	t := NewCat()
	deser, err := compiler.CompileSchemaBytes([]byte(containerReader.AvroContainerSchema()), []byte(t.Schema()))
	if err != nil {
		return nil, err
	}

	return &CatReader{
		r: containerReader,
		p: deser,
	}, nil
}

func (r CatReader) Read() (Cat, error) {
	t := NewCat()
	err := vm.Eval(r.r, r.p, &t)
	return t, err
}