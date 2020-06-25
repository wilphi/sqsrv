package sqprofile

// Commiter interface for transactions.
type Commiter interface {
	Begin(profile *SQProfile) error
	Commit(profile *SQProfile) error
	Rollback(profile *SQProfile) error
}
