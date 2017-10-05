// Copyright 2017 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package batch

import (
	"fmt"
	"testing"
)

func TestBatchSplit(t *testing.T) {
	type testItem struct {
		SQL    string
		Expect []string
	}

	list := []testItem{
		testItem{
			SQL: `use DB
go
select 1
go
select 2
`,
			Expect: []string{`use DB
`, `
select 1
`, `
select 2
`,
			},
		},
		testItem{
			SQL: `go
use DB go
`,
			Expect: []string{`
use DB go
`,
			},
		},
		testItem{
			SQL: `select 'It''s go time'
go
select top 1 1`,
			Expect: []string{`select 'It''s go time'
`, `
select top 1 1`,
			},
		},
		testItem{
			SQL: `select 1 /* go */
go
select top 1 1`,
			Expect: []string{`select 1 /* go */
`, `
select top 1 1`,
			},
		},
		testItem{
			SQL: `select 1 -- go
go
select top 1 1`,
			Expect: []string{`select 1 -- go
`, `
select top 1 1`,
			},
		},
		testItem{SQL: `"0'"`, Expect: []string{`"0'"`}},
		testItem{SQL: "0'", Expect: []string{"0'"}},
		testItem{SQL: "--", Expect: []string{"--"}},
		testItem{SQL: "GO", Expect: nil},
		testItem{SQL: "/*", Expect: []string{"/*"}},
		testItem{SQL: "gO\x01\x00O550655490663051008\n", Expect: []string{"\n"}},
		testItem{SQL: "select 1;\nGO  2\nselect 2;", Expect: []string{"select 1;\n", "select 1;\n", "\nselect 2;"}},
		testItem{SQL: "select 'hi\\\n-hello';", Expect: []string{"select 'hi-hello';"}},
		testItem{SQL: "select 'hi\\\r\n-hello';", Expect: []string{"select 'hi-hello';"}},
		testItem{SQL: "select 'hi\\\r-hello';", Expect: []string{"select 'hi-hello';"}},
		testItem{SQL: "select 'hi\\\n\nhello';", Expect: []string{"select 'hi\nhello';"}},
	}

	index := -1

	for i := range list {
		if index >= 0 && index != i {
			continue
		}
		sqltext := list[i].SQL
		t.Run(fmt.Sprintf("index-%d", i), func(t *testing.T) {
			ss := Split(sqltext, "go")
			if len(ss) != len(list[i].Expect) {
				t.Errorf("Test Item index %d; expect %d items, got %d %q", i, len(list[i].Expect), len(ss), ss)
				return
			}
			for j := 0; j < len(ss); j++ {
				if ss[j] != list[i].Expect[j] {
					t.Errorf("Test Item index %d, batch index %d; expect <%s>, got <%s>", i, j, list[i].Expect[j], ss[j])
				}
			}
		})
	}
}

func TestHasPrefixFold(t *testing.T) {
	list := []struct {
		s, pre string
		is     bool
	}{
		{"h", "H", true},
		{"h", "K", false},
		{"go 5\n", "go", true},
	}
	for _, item := range list {
		is := hasPrefixFold(item.s, item.pre)
		if is != item.is {
			t.Error("want (%q, %q)=%t got %t", item.s, item.pre, item.is, is)
		}
	}
}
