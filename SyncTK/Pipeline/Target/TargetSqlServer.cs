using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using SyncTK;
using System.Threading.Tasks;

namespace SyncTK
{
    public class TargetSqlServer : ConnectorSqlServer
    {
        protected int _batchSize = 50000;       // intelligently set this based on environment & configuration?
        protected bool _create;
        protected bool _overwrite;

        public TargetSqlServer(string connectionString, string schema, string table, bool create = true, bool overwrite = false, int _timeout = 3600)
        {
            _connectionString = connectionString;
            _schema = schema;
            _table = table;
            _create = create;
            _overwrite = overwrite;
        }

        public TargetSqlServer(string server, string database, string schema, string table, bool create = true, bool overwrite = false, int _timeout = 3600)
        {
            _server = server;
            _database = database;
            _schema = schema;
            _table = table;
            _create = create;
            _overwrite = overwrite;
        }

        internal override void Validate(Sync pipeline, Component upstreamComponent)
        {
            Assert(!(upstreamComponent.GetType().Name.Contains("Convert")), "Cannot use converters with this target.");
        }

        internal override IEnumerable<object> Process(Sync pipeline, Component upstreamComponent, IEnumerable<object> input)
        {
            SqlConnection conn = null;

            try
            {
                conn = new SqlConnection(GetConnectionString());
                _connections.Add(conn);
                conn.Open();

                // Import data from all input readers asynchronously.
                var blk = new SqlBulkCopy(conn.ConnectionString, SqlBulkCopyOptions.TableLock)
                {
                    DestinationTableName = $"[{_schema}].[{_table}]",
                    BulkCopyTimeout = _timeout,
                    BatchSize = _batchSize
                };

                bool initialized = false;
                var blkTasks = new List<Task>();
                foreach (var i in input)
                {
                    var reader = (TypeConversionReader)i;

                    // Initialize on first enumeration.
                    if (!initialized)
                    {
                        initialized = true;

                        // Configure type conversion reader with target information.
                        reader.SetTarget(this.GetType());

                        // If create flag is set, run the create script.
                        if (_create)
                        {
                            this.CreateTargetTable(conn, reader.ConversionTable, _schema, _table, _overwrite);
                        }
                    }

                    blk.WriteToServer(reader);
                }

                // Targets don't produce output during processing.
                return null;
            }
            finally
            {
                if (conn != null)
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }
    }
}
