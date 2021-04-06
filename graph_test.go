package flow

import (
	"os"
	"path/filepath"
	"testing"
)

func Test_fileExists(t *testing.T) {
	cwd, _ := os.Getwd()
	type args struct {
		fn string
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{"real_file", args{filepath.Join(cwd, "graph_test.go")}, true, false},
		{"missing_file", args{filepath.Join(cwd, "graph_test.c")}, false, false},
		{"real_directory", args{cwd}, true, false},
		{"no_permission", args{"/root/file.txt"}, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fileExists(tt.args.fn)
			if (err != nil) != tt.wantErr {
				t.Errorf("fileExists() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("fileExists() = %v, want %v", got, tt.want)
			}
		})
	}
}
