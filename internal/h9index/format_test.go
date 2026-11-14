package h9index_test

import (
	"strings"
	"testing"

	"9fans.net/go/plan9"

	"github.com/progrium/wanix-cache/internal/h9index"
)

func dir(path string, qp uint64) h9index.Entry {
	return h9index.Entry{Path: path, Dir: plan9.Dir{Qid: plan9.Qid{Path: qp, Type: plan9.QTDIR},
		Mode: plan9.DMDIR | 0o775, Mtime: 1730851200, Uid: "sys", Gid: "sys"}}
}

func TestEncodeMatchesSpecExample(t *testing.T) {
	ls := h9index.Entry{Path: "/bin/ls", Dir: plan9.Dir{Qid: plan9.Qid{Path: 0x2a1f, Vers: 3},
		Mode: 0o775, Length: 81920, Mtime: 1730851200, Uid: "glenda", Gid: "sys"}}
	root := dir("/", 1)
	root.Qid.Vers = 7
	bin := dir("/bin", 2)
	bin.Qid.Vers = 3
	got := string(h9index.Encode([]h9index.Entry{ls, bin, root}, nil))
	want := `h9index 1
/ 0000000000000001 7 d 020000000775 0 1730851200 sys sys
/bin 0000000000000002 3 d 020000000775 0 1730851200 sys sys
/bin/ls 0000000000002a1f 3 - 0775 81920 1730851200 glenda sys
`
	if got != want {
		t.Fatalf("got\n%s\nwant\n%s", got, want)
	}
	x, err := h9index.Parse(strings.NewReader(got))
	if err != nil {
		t.Fatal(err)
	}
	if e, ok := x.Lookup("/bin/ls"); !ok || e.Length != 81920 || e.Qid.Path != 0x2a1f || e.Uid != "glenda" {
		t.Fatalf("lookup /bin/ls: %+v %v", e, ok)
	}
}

func TestEncodeSortsByFullPath(t *testing.T) {
	// Byte order puts /a-b (0x2d) before /a/x (0x2f): not tree-walk order.
	es := []h9index.Entry{dir("/", 1), dir("/a", 2), dir("/a/x", 3), dir("/a-b", 4)}
	b := h9index.Encode(es, []string{"/a"})
	var paths []string
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n")[1:] {
		if !strings.HasPrefix(l, "!") {
			l = strings.SplitN(l, " ", 2)[0]
		}
		paths = append(paths, l)
	}
	got := strings.Join(paths, ",")
	if want := "/,/a,!unindexed /a,/a-b,/a/x"; got != want {
		t.Fatalf("order %s, want %s", got, want)
	}
	x, err := h9index.ParseBytes(b)
	if err != nil {
		t.Fatal(err)
	}
	if !x.Unindexed["/a"] || len(x.Entries) != 4 {
		t.Fatalf("parsed %+v", x)
	}
}

func TestParseRejects(t *testing.T) {
	for name, text := range map[string]string{
		"header":       "h9index 2\n/ 1 0 d 20000000555 0 0 u g\n",
		"fields":       "h9index 1\n/ 1 0 d 20000000555 0 0 u\n",
		"order":        "h9index 1\n/ 1 0 d 20000000555 0 0 u g\n/b 2 0 - 444 0 0 u g\n/a 3 0 - 444 0 0 u g\n",
		"orphan":       "h9index 1\n/ 1 0 d 20000000555 0 0 u g\n/a/b 2 0 - 444 0 0 u g\n",
		"file parent":  "h9index 1\n/ 1 0 d 20000000555 0 0 u g\n/a 2 0 - 444 0 0 u g\n/a/b 3 0 - 444 0 0 u g\n",
		"no root":      "h9index 1\n/a 1 0 d 20000000555 0 0 u g\n",
		"bad unindex":  "h9index 1\n/ 1 0 d 20000000555 0 0 u g\n!unindexed /x\n",
		"qid type":     "h9index 1\n/ 1 0 x 20000000555 0 0 u g\n",
		"empty":        "",
		"only header":  "h9index 1\n",
		"octal mode":   "h9index 1\n/ 1 0 d 9 0 0 u g\n",
		"hex qid path": "h9index 1\n/ zz 0 d 20000000555 0 0 u g\n",
	} {
		if _, err := h9index.Parse(strings.NewReader(text)); err == nil {
			t.Errorf("%s: parsed", name)
		}
	}
}

func TestValidName(t *testing.T) {
	for name, ok := range map[string]bool{"ls": true, "a b": false, "a\nb": false, "\xff": false, "": false, "ü": true} {
		if h9index.ValidName(name) != ok {
			t.Errorf("ValidName(%q) != %v", name, ok)
		}
	}
}
