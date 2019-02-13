using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SyncTK
{
    internal class SqlServerDataWriter
    {
        protected int _batchSize = 0;
        protected SqlConnection _conn;
        protected int _timeout;
        protected string _schema;
        protected string _table;
        protected bool _initialized = false;
        protected bool _create;
        protected bool _overwrite;
        protected TypeConversionTable _typeConversionTable;

        public SqlServerDataWriter(SqlConnection conn, string schema, string table, bool create, bool overwrite, int timeout, int batchSize)
        {
            _conn = conn;
            _schema = schema;
            _table = table;
            _timeout = timeout;
            _batchSize = batchSize;
            _create = create;
            _overwrite = overwrite;
        }

        public bool Write(IDataReader reader)
        {
            // Import data from all input readers asynchronously.
            var blk = new SqlBulkCopy(_conn.ConnectionString, SqlBulkCopyOptions.TableLock)
            {
                DestinationTableName = $"[{_schema}].[{_table}]",
                BulkCopyTimeout = _timeout,
                BatchSize = _batchSize
            };

            CreateTypeConversionTable(reader);
            var typeConversionReader = new TypeConversionReader(reader, _typeConversionTable);

            // If create flag is set, run the create script.
            if (_create)
            {
                CreateTargetTable();
            }

            // Bulk insert via SqlBulkCopy
            blk.WriteToServer(typeConversionReader);

            return true;
        }

        protected void CreateTypeConversionTable(IDataReader reader)
        {
            _typeConversionTable = new TypeConversionTable(reader.GetSchemaTable());

            foreach (var map in _typeConversionTable)
            {
                // A variable length type with a size greater than 8K becomes MAX in SQL Server.
                if ((map.SourceDataTypeName.Contains("CHAR") || map.SourceDataTypeName.Contains("BINARY")) && map.SourceColumnSize > 8000)
                {
                    map.SourceColumnSize = -1;
                    map.TargetColumnSize = -1;
                }

                // .NET strings convert to NVARCHAR(MAX)
                if (map.SourceDataTypeName == "STRING")
                {
                    map.TargetDataTypeName = "NVARCHAR";
                    map.TargetColumnSize = -1;
                }

                // SQL Server special types (Geography, Geometry) require special assembly. However, if these are
                // converted to BINARY the assembly isn't required and SQL will convert in target.
                switch (map.SourceDataTypeName)
                {
                    case "GEOGRAPHY":
                        map.TransportAsBinary = true;
                        break;
                    case "GEOMETRY":
                        map.TransportAsBinary = true;
                        break;
                    case "HIERARCHYID":
                        map.TransportAsBinary = true;
                        break;
                }
            }
        }

        internal void CreateTargetTable()
        {
            // Build a table create script using the provided schema information.  Should automatically check
            // if table exists before executing.
            string columnSQL = "";
            foreach (var c in _typeConversionTable)
            {
                columnSQL +=
                    $"[{c.TargetColumnName}] [" + c.TargetDataTypeName + "] "
                    + ""
                    + IIF(c.TargetDataTypeName.Contains("CHAR") && c.TargetColumnSize == -1, "(MAX)")
                    + IIF(c.TargetDataTypeName.Contains("CHAR") && c.TargetColumnSize > 0, $"({c.TargetColumnSize.ToString()})")
                    + IIF(c.TargetDataTypeName == "DECIMAL", $"({c.TargetNumericPrecision.ToString()}, {c.TargetNumericScale.ToString()})")
                    + IIF(c.TargetDataTypeName == "NUMERIC", $"({c.TargetNumericPrecision.ToString()}, {c.TargetNumericScale.ToString()})")
                    + IIF(c.TargetDataTypeName.Contains("BINARY") && c.TargetColumnSize == -1, "MAX")
                    + IIF(c.TargetDataTypeName.Contains("BINARY") && c.TargetColumnSize > 0, $"({c.TargetColumnSize.ToString()})")
                    + IIF(c.TargetDataTypeName == "DATETIME2", $"({c.TargetNumericScale.ToString()})")
                    + ""
                    + IIF(c.TargetAllowNull, "NULL", "NOT NULL")
                    + ",";
            }

            var createSQL = $"CREATE TABLE [{_schema}].[{_table}]( {columnSQL} )";
            var dropSQL = _overwrite ? $"DROP TABLE IF EXISTS {_schema}].[{_table}]( {columnSQL} )" : "";
            var script = $"IF OBJECT_ID('[{_schema}].[{_table}]') IS NULL{Environment.NewLine}    {createSQL}";

            // Execute the command.
            var cmd = _conn.CreateCommand();
            cmd.CommandText = script;
            cmd.ExecuteNonQuery();
        }

        internal string IIF(bool condition, string truePart, string falsePart = "")
        {
            if (condition)
                return truePart;
            else
                return falsePart;
        }
    }
}
