package subsys

import (
	"context"

	"github.com/progrium/wanix-cache/internal/meta"
)

// Viewer is optionally implemented by subsystems that serve composed,
// metadata-only trees under their namespace root besides their units: an
// OCI image's merged rootfs, a Nix closure.
//
// A view's directories are virtual: their inode numbers have VirtualBit
// set and exist only in the subsystem. Everything else a view returns (files,
// symlinks) is a real inode of some unit, so opens go through the fill
// path as usual and fault evicted units back in.
type Viewer interface {
	// ViewLookup resolves name in a directory. dir is a virtual inode from
	// an earlier call, or the real inode of the subsystem root (/<name>/).
	// ok is false when the name is not part of a view, so the caller falls
	// back to the real namespace.
	ViewLookup(ctx context.Context, dir uint64, name string) (in meta.Inode, ok bool, err error)
	// ViewGet returns a virtual directory's attributes. Its Parent is the
	// virtual or real inode it was looked up in.
	ViewGet(ctx context.Context, ino uint64) (meta.Inode, error)
	// ViewReadDir lists a virtual directory in name order. Whiteouts are
	// already applied and never returned.
	ViewReadDir(ctx context.Context, ino uint64) ([]meta.Dirent, error)
}

// VirtualBit is set in every virtual inode number and in no real one.
// Virtual numbers stay below 1<<62: the two top bits are reserved for
// synthetic per-root files such as .h9index.
const VirtualBit uint64 = 1 << 61

// VirtualMask covers the bits a virtual inode number may use besides
// VirtualBit.
const VirtualMask uint64 = VirtualBit - 1

// IsVirtual reports whether ino is a view's virtual directory.
func IsVirtual(ino uint64) bool { return ino&^VirtualMask == VirtualBit }
