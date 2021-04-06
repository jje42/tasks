package flow

import "testing"

func Test_convertMemory(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name    string
		args    args
		want    int
		wantErr bool
	}{
		// TODO: Add test cases.
		{"t1", args{"1gb"}, 1, false},
		{"t2", args{"1024mb"}, 1, false},
		{"t3", args{"1048576kb"}, 1, false},
		{"t4", args{"1048576b"}, 0, true},
		{"t5", args{"0"}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertMemory(tt.args.s)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertMemory() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("convertMemory() = %v, want %v", got, tt.want)
			}
		})
	}
}
