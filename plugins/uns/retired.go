package uns

// retiredContracts are contracts colca no longer defines whose records can
// still exist: retained state in stores written by an older node, and stream
// records until retention removes them. A node purges their retained state at
// startup and drops their records when a child replicates them.
//
// Application time (_ClockDefinition, _ClockProgress) was retired in 0.34.0.
// Remove the entries, and the purge and drop that use them, once every node has
// started on 0.34 or later and the entities retention (8760 h) has passed.
var retiredContracts = map[string]bool{
	"_ClockDefinition": true,
	"_ClockProgress":   true,
}

// IsRetired reports whether contract is a retired contract.
func IsRetired(contract string) bool { return retiredContracts[contract] }

// RetiredTopic reports whether topic is a record of a retired contract.
func RetiredTopic(topic string) bool {
	p, err := Parse(topic)
	return err == nil && IsRetired(p.Contract)
}
