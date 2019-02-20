-- If overwrite specified, drop the target table first.
IF '{3}' = 'True'
BEGIN
	DROP TABLE IF EXISTS [{0}].[{1}]
END

-- If the table already exists, do not create. If it wasn't dropped above, then assume we're appending.
IF OBJECT_ID('[{0}].[{1}]') IS NULL
BEGIN
	-- Create table, adding compression if specified (and supported).
	IF (CAST(SERVERPROPERTY('Edition') AS VARCHAR(100)) LIKE 'Enterprise%' OR CAST(SERVERPROPERTY('Edition') AS VARCHAR(100)) LIKE 'Developer%') AND '{4}' = 'True'
	BEGIN
		CREATE TABLE [{0}].[{1}]({2}) WITH (DATA_COMPRESSION = PAGE)
	END
	ELSE
	BEGIN
		CREATE TABLE [{0}].[{1}]({2})
	END
END