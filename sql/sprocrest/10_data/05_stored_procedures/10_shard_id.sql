CREATE FUNCTION test_data.get_shard_id() RETURNS TEXT
IMMUTABLE
LANGUAGE SQL
AS $$select coalesce(substring(current_database() from E'\\d+$'), '')$$;
