package params

// Overhead is opt-in. The legacy entry points are unchanged when Enabled is false.
var Overhead OverheadConfig

type OverheadConfig struct {
	Enabled               bool
	Lightweight           bool
	Migration             bool
	MigrationAfterSeconds int
	MigrationAccounts     int
	TimeoutSeconds        int
	// Skip streaming to this replica to exercise real on-demand recovery; -1 disables.
	StreamSkipNode int
}
