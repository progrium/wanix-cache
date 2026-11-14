package savings

import "syscall"

func filesystem(dir string) Filesystem {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return Filesystem{Type: "unknown", BlockSize: 4096}
	}
	names := map[int64]string{
		0xef53: "ext4", 0x58465342: "xfs", 0x9123683e: "btrfs", 0x01021994: "tmpfs",
		0x2fc12fc1: "zfs", 0x794c7630: "overlayfs", 0x6969: "nfs", 0x65735546: "fuse",
		0xf2f52010: "f2fs", 0x01021997: "9p",
	}
	t, ok := names[int64(st.Type)]
	if !ok {
		t = "0x" + itoaHex(uint64(st.Type))
	}
	return Filesystem{Type: t, BlockSize: int64(st.Bsize)}
}

func itoaHex(n uint64) string {
	const digits = "0123456789abcdef"
	if n == 0 {
		return "0"
	}
	var b []byte
	for ; n > 0; n >>= 4 {
		b = append([]byte{digits[n&15]}, b...)
	}
	return string(b)
}
