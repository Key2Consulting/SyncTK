using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using SyncTK;

namespace SyncTK
{
    public class SourceSqlServer : ConnectorSqlServer, ISource
    {

        public SourceSqlServer(string server, string database, string schema, string table, int timeout = 3600)
        {
            _server = server;
            _database = database;
            _schema = schema;
            _table = table;
            _timeout = timeout;
            _query = $"SELECT * FROM [{schema}].[{table}]";
        }

        public SourceSqlServer(string server, string database, string query)
        {
            _server = server;
            _database = database;
            _query = query;
        }

        internal override IEnumerable<object> Process(Sync pipeline, Component upstreamComponent, IEnumerable<object> input)
        {
            SqlConnection conn = null;

            try
            {
                conn = new SqlConnection($"Server={_server};Integrated Security=true;Database={_database}");
                _connections.Add(conn);
                conn.Open();
                var cmd = conn.CreateCommand();
                cmd.CommandText = _query;
                cmd.CommandTimeout = _timeout;
                yield return new TypeConversionReader(cmd.ExecuteReader(), this.GetType());
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