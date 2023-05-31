// Copyright 2023 Jonathan Ellis
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tasks

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
