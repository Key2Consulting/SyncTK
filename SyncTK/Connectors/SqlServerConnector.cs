using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using SyncTK;

namespace SyncTK
{
    public class SqlServerConnector : DatabaseConnector
    {
        public static SqlServerConnector From()
        {
            return new SqlServerConnector();
        }

        public override IDataReader Export(string query)
        {
            return base.Export(query);
        }

        public override IDataReader Export(string schemaName, string tableName)
        {
            return base.Export(schemaName, tableName);
        }

        public override IDataReader Export(QueryTemplate query)
        {
            return base.Export(query);
        }

        public override IDataReader Import(IDataReader reader, string schemaName, string tableName)
        {
            return base.Import(reader, schemaName, tableName);
        }
    }
}