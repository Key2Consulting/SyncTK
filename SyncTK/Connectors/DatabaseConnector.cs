using System;
using System.Data;
using SyncTK;

namespace SyncTK
{
    public class DatabaseConnector : Connector
    {
        public virtual IDataReader Export(string query)
        {
            throw new NotImplementedException();
        }

        public virtual IDataReader Export(string schemaName, string tableName)
        {
            throw new NotImplementedException();
        }

        public virtual IDataReader Export(QueryTemplate query)
        {
            throw new NotImplementedException();
        }

        public virtual IDataReader Import(IDataReader reader, string schemaName, string tableName)
        {
            throw new NotImplementedException();
        }

        public QueryTemplate ScriptAsTemplate(DataTable schemaTable)
        {
            throw new NotImplementedException();
        }
    }
}