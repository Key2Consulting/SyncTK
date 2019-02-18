using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using SyncTK.Internal;
using System.Threading.Tasks;

namespace SyncTK
{
    public class TargetSqlServer : Target
    {
        protected int _batchSize = 0;
        protected bool _create;
        protected bool _overwrite;
        protected string _connectionString;
        protected string _server;
        protected string _database;
        protected string _schema;
        protected string _table;
        protected int _timeout;
        protected string _query;
        protected SqlConnection _connection;

        public TargetSqlServer(string connectionString, string schema, string table, bool create = true, bool overwrite = false, int timeout = 3600, int batchSize = 10000)
        {
            _connectionString = connectionString;
            _schema = schema;
            _table = table;
            _create = create;
            _overwrite = overwrite;
            _batchSize = batchSize;
        }

        public TargetSqlServer(string server, string database, string schema, string table, bool create = true, bool overwrite = false, int timeout = 3600, int batchSize = 10000)
        {
            _server = server;
            _database = database;
            _schema = schema;
            _table = table;
            _create = create;
            _overwrite = overwrite;
            _batchSize = batchSize;
        }

        internal override void Validate()
        {
            var upstreamComponent = GetUpstreamComponent();
            Assert(_pipeline.FindComponentType<WriteFormatter>() == null, "Cannot use write formatters with a database target.");
        }

        internal override IEnumerable<object> Process(IEnumerable<object> input)
        {
            using (_connection = new SqlConnection(GetConnectionString()))
            {
                _connection.Open();

                foreach (var i in input)
                {
                    var reader = (IDataReader)i;

                    // Import data from all input readers asynchronously.
                    var blk = new SqlBulkCopy(_connection.ConnectionString, SqlBulkCopyOptions.TableLock)
                    {
                        DestinationTableName = $"[{_schema}].[{_table}]",
                        BulkCopyTimeout = _timeout,
                        BatchSize = _batchSize
                    };

                    // If create flag is set, run the create script.
                    if (_create)
                    {
                        CreateTargetTable();
                    }

                    // Bulk insert via SqlBulkCopy
                    blk.WriteToServer(reader);
                }
            }

            // Targets don't produce output during processing.
            return null;
        }

        protected string GetConnectionString()
        {
            if (_connectionString != null)
                return _connectionString;
            else
                return $"Server ={ _server}; Integrated Security = true; Database ={ _database}";
        }

        protected void CreateTargetTable()
        {
            // Build a table create script using the provided schema information.  Should automatically check
            // if table exists before executing.
            string columnSQL = "";
            foreach (var c in GetTypeConversionTable())
            {
                var dataTypeName = c.Target.DataTypeName.ToUpper();
                columnSQL +=
                    $"[{c.Target.ColumnName}] [" + dataTypeName + "] "
                    + ""
                    + Util.IIF(dataTypeName.Contains("CHAR") && c.Target.ColumnSize == -1, "(MAX)")
                    + Util.IIF(dataTypeName.Contains("CHAR") && c.Target.ColumnSize > 0, $"({c.Target.ColumnSize.ToString()})")
                    + Util.IIF(dataTypeName == "DECIMAL", $"({c.Target.NumericPrecision.ToString()}, {c.Target.NumericScale.ToString()})")
                    + Util.IIF(dataTypeName == "NUMERIC", $"({c.Target.NumericPrecision.ToString()}, {c.Target.NumericScale.ToString()})")
                    + Util.IIF(dataTypeName.Contains("BINARY") && c.Target.ColumnSize == -1, "(MAX)")
                    + Util.IIF(dataTypeName.Contains("BINARY") && c.Target.ColumnSize > 0, $"({c.Target.ColumnSize.ToString()})")
                    + Util.IIF(dataTypeName == "DATETIME2", $"({c.Target.NumericScale.ToString()})")
                    + ""
                    + Util.IIF(c.Target.AllowNull, " NULL", " NOT NULL")
                    + ",";
            }

            var createSQL = $"CREATE TABLE [{_schema}].[{_table}]( {columnSQL} ) WITH (DATA_COMPRESSION = PAGE)";
            var dropSQL = _overwrite ? $"DROP TABLE IF EXISTS {_schema}].[{_table}]( {columnSQL} )" : "";
            var script = $"IF OBJECT_ID('[{_schema}].[{_table}]') IS NULL{Environment.NewLine}    {createSQL}";

            // Execute the command.
            var cmd = _connection.CreateCommand();
            cmd.CommandText = script;
            cmd.ExecuteNonQuery();
        }
    }
}