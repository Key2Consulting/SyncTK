using System;
using System.Data.SqlClient;
using System.IO;
using System.Reflection;
using System.Threading;
using Xunit;
using Xunit.Runners;

namespace SyncTK.Test
{
    class Program
    {
        static int Main(string[] args)
        {
            // Global Setup
            GlobalSetup();

            // Test Execution
            var runner = new Runner();
            string[] tests = {
                "SyncTK.Test.UnitTests.FileTests",
                "SyncTK.Test.UnitTests.SqlServerTests",
                "SyncTK.Test.UnitTests.AzureTests"
            };
            var r = runner.Run(tests);

            // Force a keypress to allow viewing of results.
            Console.ForegroundColor = ConsoleColor.White;
            Console.WriteLine("Test Complete (press any key).");
            Console.ReadKey();

            // Global Teardown
            GlobalTearDown();

            return r;
        }

        static void GlobalSetup()
        {
            // Cleanup Temp Files from Last Run
            var cfg = new SyncTK.Test.UnitTests.TestBase();
            System.IO.DirectoryInfo di = new DirectoryInfo(cfg.GetConfig("TempFilesRoot"));
            foreach (FileInfo file in di.GetFiles())
            {
                file.Delete();
            }
            
            // Setup SQL Server Test Database
            string sql = @"
                IF EXISTS(SELECT * FROM sys.databases WHERE [name] = 'SyncTK')
                BEGIN
                    EXEC sp_executesql N'ALTER DATABASE [SyncTK] SET SINGLE_USER WITH ROLLBACK IMMEDIATE'
                    EXEC sp_executesql N'DROP DATABASE [SyncTK]'
                END
                CREATE DATABASE [SyncTK]
                
                CREATE TABLE [SyncTK].[dbo].[OddTypes](
	                [ID] [int] NULL,
	                [Geography] [geography] NULL,
	                [Xml] [xml] NULL,
	                [Binary] [binary](50) NULL,
	                [DateTime2] [datetime2](7) NULL,
	                [HierarchyID] [hierarchyid] NULL,
	                [Geometry] [geometry] NULL,
	                [SmallMoney] [smallmoney] NULL,
	                [TimeStamp] [timestamp] NULL
                )

                INSERT INTO [SyncTK].[dbo].[OddTypes]
                VALUES
	                (
		                1
		                ,geography::Point(47.65100, -122.34900, 4326)
		                ,'<xml></xml>'
		                ,CAST('hello world' AS BINARY(50))
		                ,GETDATE()
		                ,'/1/'
		                ,geometry::STGeomFromText('LINESTRING (100 100, 20 180, 180 180)', 0)
		                ,44.11
		                ,NULL
	                )";
            
            using (var c = new SqlConnection(@"Server=(LocalDb)\MSSQLLocalDB;Integrated Security=true;Database=master"))
            {
                c.Open();
                var cmd = c.CreateCommand();
                cmd.CommandText = sql;
                cmd.ExecuteNonQuery();
            }
        }

        static void GlobalTearDown()
        {

        }
    }
}