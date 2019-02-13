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
        protected int _batchSize = 0;       // intelligently set this based on environment & configuration?
        protected bool _create;
        protected bool _overwrite;

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

                var writer = new SqlServerDataWriter(conn, _schema, _table, _create, _overwrite, _timeout, _batchSize);
                foreach (var i in input)
                {
                    var reader = (IDataReader)i;
                    writer.Write(reader);
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
