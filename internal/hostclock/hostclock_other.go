//go:build !linux

package hostclock

// Read reports nothing: only the Linux kernel exposes the verdict colca reads.
func Read() Status { return Status{} }
