using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using SyncTK.Internal;

namespace SyncTK
{
    public class SourceSqlServer : Source
    {
        protected string _connectionString;
        protected string _server;
        protected string _database;
        protected string _schema;
        protected string _table;
        protected int _timeout;
        protected string _query;
        protected SqlConnection _connection;

        public SourceSqlServer(string connectionString, string schema, string table, int timeout = 3600)
        {
            _connectionString = connectionString;
            _schema = schema;
            _table = table;
            _timeout = timeout;
            _query = $"SELECT * FROM [{schema}].[{table}]";
        }

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

        internal override IEnumerable<object> Process(IEnumerable<object> input)
        {
            using (_connection = new SqlConnection(GetConnectionString()))
            {
                _connection.Open();
                var cmd = _connection.CreateCommand();
                cmd.CommandText = _query;
                cmd.CommandTimeout = _timeout;
                yield return cmd.ExecuteReader();
            }
        }

        protected string GetConnectionString()
        {
            if (_connectionString != null)
                return _connectionString;
            else
                return $"Server ={ _server}; Integrated Security = true; Database ={ _database}";
        }
    }
}