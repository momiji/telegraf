package rollingfile

import (
	"fmt"
	"io"
	"os"
    "time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs"
	"github.com/influxdata/telegraf/plugins/serializers"
)

type RollingFile struct {
	Files []string
    
    currentFiles []string
    writers[] io.Writer

	writer  io.Writer
	closers []io.Closer

	serializer serializers.Serializer
}

var sampleConfig = `
  ## Files to write to, "stdout" is a specially handled file.
  files = ["stdout", "/tmp/metrics.out", "/tmp/metrics-%Y%m%D-%H%M%S"]

  ## Data format to output.
  ## Each data format has it's own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md
  data_format = "influx"
`

func (f *RollingFile) SetSerializer(serializer serializers.Serializer) {
	f.serializer = serializer
}

func (f *RollingFile) Connect() error {
	if len(f.Files) == 0 {
		f.Files = []string{"stdout"}
	}
    
    var l = len(f.Files)
    
    f.currentFiles = make([]string, l)
    f.writers = make([]io.Writer, l)
    f.closers = make([]io.Closer, l)
    
    return nil
}

func (f *RollingFile) ReConnect() error {
    var errS string
    var update = false
	for index, file := range f.Files {
		if file == "stdout" {
            if f.writers[index] == nil {
                f.writers[index] = os.Stdout
                f.closers[index] = os.Stdout
            }
		} else {
			var of *os.File
			var err error
            file = time.Now().UTC().Format(file)
            if file != f.currentFiles[index] {
                update = true
                if f.writers[index] != nil {
                    if err := f.closers[index].Close(); err != nil {
                        errS += err.Error() + "\n"
                    }
                    f.writers[index] = nil
                    f.closers[index] = nil
                }
                if _, err := os.Stat(file); os.IsNotExist(err) {
                    of, err = os.Create(file)
                } else {
                    of, err = os.OpenFile(file, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
                }

                if err != nil {
                    errS += err.Error() + "\n"
                } else {
                    f.writers[index] = of
                    f.closers[index] = of
                }
            }
		}
	}
    if (update) {
        f.writer = io.MultiWriter(f.writers...)
    }
    if (errS != "") {
        return fmt.Errorf(errS)
    }
	return nil
}

func (f *RollingFile) Close() error {
	var errS string
	for _, c := range f.closers {
		if err := c.Close(); err != nil {
			errS += err.Error() + "\n"
		}
	}
	if errS != "" {
		return fmt.Errorf(errS)
	}
	return nil
}

func (f *RollingFile) SampleConfig() string {
	return sampleConfig
}

func (f *RollingFile) Description() string {
	return "Send telegraf metrics to file(s), allowing time layouts in file name"
}

func (f *RollingFile) Write(metrics []telegraf.Metric) error {
	if len(metrics) == 0 {
		return nil
	}
    
    f.ReConnect()
    
	for _, metric := range metrics {
		values, err := f.serializer.Serialize(metric)
		if err != nil {
			return err
		}

		for _, value := range values {
			_, err = f.writer.Write([]byte(value + "\n"))
			if err != nil {
				return fmt.Errorf("FAILED to write message: %s, %s", value, err)
			}
		}
	}
	return nil
}

func init() {
	outputs.Add("rollingfile", func() telegraf.Output {
		return &RollingFile{}
	})
}
