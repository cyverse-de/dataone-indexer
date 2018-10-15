package database

// Event types.
const (
	ETCreate                = "CREATE"
	ETDelete                = "DELETE"
	ETRead                  = "READ"
	ETReplicate             = "REPLICATE"
	ETReplicationFailed     = "REPLICATION_FAILED"
	ETSynchronizationFailed = "SYNCHRONIZATION_FAILED"
	ETUpdate                = "UPDATE"
)

// The statement used to add an event to the database.
const addEvent = `
INSERT INTO event_log (permanent_id, irods_path, event, date_logged, node_identifier)
VALUES ($1, $2, $3, $4, $5);
`
