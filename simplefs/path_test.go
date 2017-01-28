package simplefs

import (
	"fmt"
	"os"
	"testing"
)

func TestInvalidPathErr(t *testing.T) {
	p := P{"foo", "bar", "foo\uFFFFbar"}
	err := p.Validate()
	if err != ErrInvalidPath {
		t.Error("expected ErrInvalidPath")
	}
}

func TestValidPath(t *testing.T) {
	p := P{"foo", "bar"}
	if len(p) != 2 {
		t.Error("expected path with this many components")
	}

	err := p.Validate()
	if err != nil {
		t.Error("expected path to be valid")
	}
}

func TestPathStringer(t *testing.T) {
	p := P{"foo", "bar"}

	str1 := fmt.Sprintf("%s", p)
	if str1 != "/foo/bar" {
		t.Errorf("expected correct string, got: %v", str1)
	}

	str2 := fmt.Sprintf("%s", P{})
	if str2 != "/" {
		t.Errorf("expected correct string, got: %v", str2)
	}
}

func TestPathErr(t *testing.T) {
	p := P{"foo", "bar"}

	perr := p.Err("stat", os.ErrNotExist)
	if !os.IsNotExist(perr) {
		t.Error("expected path error to be accepted by os.IsNotExist")
	}
}

func TestPathBase(t *testing.T) {
	p1 := P{"foo"}
	if p1.Base() != "foo" {
		t.Error("expected path base to be this")
	}

	if Root.Base() != RootBasename {
		t.Error("base of the root is special")
	}
}

func TestPathParent(t *testing.T) {
	p := P{"foo", "bar"}

	parent := p.Parent()
	if fmt.Sprintf("%s", parent) != "/foo" {
		t.Errorf("expected different parent, got: %+v", parent)
	}

	root1 := parent.Parent()
	if fmt.Sprintf("%s", root1) != "/" {
		t.Errorf("expected different parent, got: %+v", root1)
	}

	root2 := parent.Parent()
	if fmt.Sprintf("%s", root2) != "/" {
		t.Errorf("expected different parent, got: %+v", root2)
	}
}

func TestPathEquality(t *testing.T) {
	b := P{"foo", "bar"}.Equals(P{"foobar"})
	if b {
		t.Error("should not be equal")
	}

	b = P{}.Equals(Root)
	if !b {
		t.Error("should be equal")
	}

	b = P{"foo", "bar"}.Equals(P{"foo", "bar"})
	if !b {
		t.Error("should be equal")
	}
}
