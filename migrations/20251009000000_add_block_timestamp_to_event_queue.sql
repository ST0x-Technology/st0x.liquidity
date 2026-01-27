-- Add block_timestamp column to event_queue table
ALTER TABLE event_queue ADD COLUMN block_timestamp TIMESTAMP;
