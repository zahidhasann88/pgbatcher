package pgbatcher

import (
	"strconv"
	"testing"
)

func TestParseInsert(t *testing.T) {
	cases := []struct {
		s            string
		parse, batch bool
	}{{`INSERT INTO t (a,b) VALUES ($1,$2)`, true, true}, {`/*x*/ insert into "T" ("A") values ($1) ON CONFLICT ("A") DO NOTHING`, true, false}, {`INSERT INTO t(a) VALUES ($1); SELECT 2`, false, false}, {`WITH x AS (SELECT 1) INSERT INTO t(a) VALUES ($1)`, false, false}, {`INSERT INTO t(a) VALUES (';')`, false, false}, {`INSERT INTO t(a) VALUES ($$x$$)`, false, false}, {`INSERT INTO t(a) VALUES ($1),($2)`, false, false}, {`INSERT INTO t(a) VALUES ($1) RETURNING a`, false, false}, {`INSERT INTO t(a) VALUES($1) /* trailing */`, true, true}, {`INSERT INTO t(a) VALUES($2)`, true, false}, {`INSERT INTO t(a) VALUES($1);`, true, true}, {`INSERT INTO t(a) VALUES($1) ON CONFLICT odd text`, true, false}}
	for _, c := range cases {
		_, p := parseInsert(c.s)
		if p != c.parse {
			t.Errorf("parseInsert(%q)=%v want %v", c.s, p, c.parse)
		}
		args := make([]any, 0)
		if p {
			shape, _ := parseInsert(c.s)
			for i := 0; i < shape.placeholderCount; i++ {
				args = append(args, 1)
			}
		}
		_, b := batchableShape(c.s, args)
		if b != c.batch {
			t.Errorf("batchableShape(%q)=%v want %v", c.s, b, c.batch)
		}
	}
}
func FuzzParseInsert(f *testing.F) {
	for _, s := range []string{`INSERT INTO t(a) VALUES($1)`, `WITH x AS (SELECT 1) INSERT INTO t(a) VALUES($1)`, `select $$;$$`, `/*x*/ INSERT INTO "t"("a") VALUES($1)`, `INSERT INTO t(a) VALUES($1) ON CONFLICT (a) DO NOTHING`, `INSERT INTO t(a) VALUES($1) RETURNING a`, `INSERT INTO t(a) VALUES($1); DELETE FROM t`} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		parseInsert(s)
		ts, err := tokenize(s)
		if err != nil {
			return
		}
		n := 0
		for _, tok := range ts {
			if tok.kind == 'p' {
				v, e := strconv.Atoi(tok.s[1:])
				if e != nil || v > 256 {
					return
				}
				if v > n {
					n = v
				}
			}
		}
		args := make([]any, n)
		shape, ok := batchableShape(s, args)
		if ok {
			if shape.placeholderCount != n {
				t.Fatalf("batchable with mismatched placeholder count: %q", s)
			}
			parsed, yes := parseInsert(s)
			if !yes || parsed.prefix == "" {
				t.Fatalf("misclassified non-INSERT as batchable: %q", s)
			}
		}
	})
}
func TestCompose(t *testing.T) {
	s, ok := batchableShape(`INSERT INTO t (a, b) VALUES ($1, $2) -- note`, []any{1, 2})
	if !ok {
		t.Fatal("not batchable")
	}
	q, args := compose(s, []*item{{args: []any{1, 2}}, {args: []any{3, 4}}})
	if want := `INSERT INTO t (a, b) VALUES($1,$2),($3,$4) -- note`; q != want {
		t.Fatalf("compose=%q want %q", q, want)
	}
	if len(args) != 4 || args[2] != 3 {
		t.Fatalf("args=%v", args)
	}
}
