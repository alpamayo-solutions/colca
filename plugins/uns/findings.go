package uns

// NodeFindingTopic is where a finding about the node itself sits:
// _Finding/{node}/{reason}. One finding per reason, overwritten while it holds
// and retired by tombstone.
func NodeFindingTopic(node, reason string) string {
	return Prefix() + "_Finding/" + node + "/" + reason
}
