-- Durable completion markers for one-time projection compatibility repairs.
-- A repair inserts its versioned key only after validation or rebuilding
-- succeeds, so an interrupted repair remains eligible on the next startup.
CREATE TABLE projection_repair_checkpoint (
    repair TEXT PRIMARY KEY
) STRICT;
