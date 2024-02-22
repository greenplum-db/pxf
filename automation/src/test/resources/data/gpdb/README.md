Data for gpdb testing `gpdb_types.txt` is generated using the SQL file called `generate_gpdb_types.sql`.

If `gpdb_types_with_date_wide_range.txt` needs to be regenerated, it is recommended to create a copy of the file to
 preserve the timezone offsets since Postgres does not store them internally. Then, manually copy those values back in
 after regenerating.
