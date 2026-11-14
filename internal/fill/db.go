package fill

import "github.com/progrium/wanix-cache/internal/meta"

// DB returns the metadata store the manager runs on, for subsystems that
// read the namespace or keep their own <name>_ tables.
func (m *LocalManager) DB() *meta.DB { return m.db }
