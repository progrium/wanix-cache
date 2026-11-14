//go:build !linux

package savings

func filesystem(dir string) Filesystem { return Filesystem{Type: "unknown", BlockSize: 4096} }
