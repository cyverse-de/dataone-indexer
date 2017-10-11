package database

// Event types.
const (
	ET_CREATE                 = "CREATE"
	ET_DELETE                 = "DELETE"
	ET_READ                   = "READ"
	ET_REPLICATE              = "REPLICATE"
	ET_REPLICATION_FAILED     = "REPLICATION_FAILED"
	ET_SYNCHRONIZATION_FAILED = "SYNCHRONIZATION_FAILED"
	ET_UPDATE                 = "UPDATE"
)

// The statement used to add an event to the database.
const addEvent = `
INSERT INTO event_log (permanent_id, irods_path, event, date_logged, node_identifier)
VALUES ($1, $2, $3, $4, $5);
`
