package gzip

import (
	"compress/gzip"
	"fmt"
	"io"

	"github.com/omeid/gonzo"
	"github.com/omeid/gonzo/context"
)

type doublecloser struct {
	io.ReadCloser
	c func() error
}

func (dc *doublecloser) Close() error {
	err := dc.ReadCloser.Close()
	errr := dc.c()
	if err == nil {
		err = errr
	} else if errr != nil {
		err = fmt.Errorf("Double Error: gzip: %s AND file: %s", err.Error(), errr.Error())
	}
	return err
}

// Untar files from input channel and pass the result
// to the output channel.
func Uncompress() gonzo.Stage {
	return func(ctx context.Context, in <-chan gonzo.File, out chan<- gonzo.File) error {

		for {
			select {
			case file, ok := <-in:
				if !ok {
					return nil
				}

				content, err := gzip.NewReader(file)
				if err != nil {
					return err
				}

				fs := gonzo.NewFile(
					&doublecloser{content, file.Close},
					gonzo.FileInfoFrom(file.FileInfo()),
				)

				out <- fs
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}
