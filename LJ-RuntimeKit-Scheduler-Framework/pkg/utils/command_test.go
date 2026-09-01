/*
Copyright (c) 2024-2026 Alibaba Cloud.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package utils

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/urfave/cli"
)

// makeCliContext creates a cli.Context with the given positional args for testing.
func makeCliContext(cmdName string, args ...string) *cli.Context {
	app := cli.NewApp()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	_ = fs.Parse(args)
	ctx := cli.NewContext(app, fs, nil)
	ctx.Command = cli.Command{Name: cmdName}
	return ctx
}

func TestCheckArgs(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		expected  int
		checkType int
		wantErr   bool
	}{
		// ExactArgs cases
		{
			name:      "ExactArgs_with_matching_count",
			args:      []string{"arg1", "arg2"},
			expected:  2,
			checkType: ExactArgs,
			wantErr:   false,
		},
		{
			name:      "ExactArgs_with_too_few_args",
			args:      []string{"arg1"},
			expected:  2,
			checkType: ExactArgs,
			wantErr:   true,
		},
		{
			name:      "ExactArgs_with_too_many_args",
			args:      []string{"arg1", "arg2", "arg3"},
			expected:  2,
			checkType: ExactArgs,
			wantErr:   true,
		},
		{
			name:      "ExactArgs_expecting_zero_args",
			args:      []string{},
			expected:  0,
			checkType: ExactArgs,
			wantErr:   false,
		},
		// MinArgs cases
		{
			name:      "MinArgs_with_more_than_minimum",
			args:      []string{"arg1", "arg2", "arg3"},
			expected:  2,
			checkType: MinArgs,
			wantErr:   false,
		},
		{
			name:      "MinArgs_with_exact_minimum",
			args:      []string{"arg1", "arg2"},
			expected:  2,
			checkType: MinArgs,
			wantErr:   false,
		},
		{
			name:      "MinArgs_with_too_few_args",
			args:      []string{"arg1"},
			expected:  2,
			checkType: MinArgs,
			wantErr:   true,
		},
		// MaxArgs cases
		{
			name:      "MaxArgs_within_limit",
			args:      []string{"arg1"},
			expected:  2,
			checkType: MaxArgs,
			wantErr:   false,
		},
		{
			name:      "MaxArgs_at_limit",
			args:      []string{"arg1", "arg2"},
			expected:  2,
			checkType: MaxArgs,
			wantErr:   false,
		},
		{
			name:      "MaxArgs_over_limit",
			args:      []string{"arg1", "arg2", "arg3"},
			expected:  2,
			checkType: MaxArgs,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := makeCliContext("testcmd", tt.args...)
			err := CheckArgs(ctx, tt.expected, tt.checkType)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "testcmd")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
