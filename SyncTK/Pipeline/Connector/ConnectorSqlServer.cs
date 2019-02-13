using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using SyncTK;

namespace SyncTK
{
    public class ConnectorSqlServer : Component
    {
        protected string _connectionString;
        protected string _server;
        protected string _database;
        protected string _schema;
        protected string _table;
        protected int _timeout;
        protected string _query;
        protected List<SqlConnection> _connections = new List<SqlConnection>();

        protected string GetConnectionString()
        {
            if (_connectionString != null)
                return _connectionString;
            else
                return $"Server ={ _server}; Integrated Security = true; Database ={ _database}";
        }

        internal override void End(Sync pipeline, Component upstreamComponent)
        {
            foreach (var conn in _connections)
            {
                conn.Close();
                conn.Dispose();
            }
        }
    }
}