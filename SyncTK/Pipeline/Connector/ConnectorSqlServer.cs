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

        internal void CreateTargetTable(SqlConnection conn, TypeConversionTable conversionTable, string schema, string table, bool overwrite)
        {
            // Build a table create script using the provided schema information.  Should automatically check
            // if table exists before executing.
            string columnSQL = "";
            foreach (var c in conversionTable)
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

            var createSQL = $"CREATE TABLE [{schema}].[{table}]( {columnSQL} )";
            var dropSQL = overwrite ? $"DROP TABLE IF EXISTS {schema}].[{table}]( {columnSQL} )" : "";
            var script = $"IF OBJECT_ID('[{schema}].[{table}]') IS NULL{Environment.NewLine}    {createSQL}";

            // Execute the command.
            var cmd = conn.CreateCommand();
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