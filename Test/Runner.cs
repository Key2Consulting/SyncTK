using System;
using System.IO;
using System.Reflection;
using System.Threading;
using Xunit;
using Xunit.Runners;

namespace SyncTK.Test
{
    // Derived from https://github.com/Microsoft/xunit-performance/blob/master/src/xunit.performance.api/XunitRunner.cs
    class Runner
    {
        // We use consoleLock because messages can arrive in parallel, so we want to make sure we get
        // consistent console output.
        static object consoleLock = new object();

        // Use an event to know when we're done
        static ManualResetEvent finished = null;

        // Start out assuming success; we'll set this to 1 if we get a failed test
        static int result = 0;

        public int Run(string[] tests)
        {
            // Get current assembly path.
            string codeBase = Assembly.GetExecutingAssembly().CodeBase;
            UriBuilder uri = new UriBuilder(codeBase);
            var testAssembly = Uri.UnescapeDataString(uri.Path);

            // Setup test framework
            // Run tests
            foreach (var typeName in tests)
            {
                using (var runner = AssemblyRunner.WithoutAppDomain(testAssembly))
                {
                    finished = new ManualResetEvent(false);

                    runner.OnDiscoveryComplete = OnDiscoveryComplete;
                    runner.OnExecutionComplete = OnExecutionComplete;
                    runner.OnTestFailed = OnTestFailed;
                    runner.OnTestSkipped = OnTestSkipped;
                    runner.Start(typeName);

                    finished.WaitOne();
                    finished.Dispose();

                    if (result != 0)
                        return result;
                }
            }

            return 0;
        }

        static void OnDiscoveryComplete(DiscoveryCompleteInfo info)
        {
            lock (consoleLock)
                Console.WriteLine($"Running {info.TestCasesToRun} of {info.TestCasesDiscovered} tests...");
        }

        static void OnExecutionComplete(ExecutionCompleteInfo info)
        {
            lock (consoleLock)
                Console.WriteLine($"Finished: {info.TotalTests} tests in {Math.Round(info.ExecutionTime, 3)}s ({info.TestsFailed} failed, {info.TestsSkipped} skipped)");

            finished.Set();
        }

        static void OnTestFailed(TestFailedInfo info)
        {
            lock (consoleLock)
            {
                Console.ForegroundColor = ConsoleColor.Red;

                Console.WriteLine("[FAIL] {0}: {1}", info.TestDisplayName, info.ExceptionMessage);
                if (info.ExceptionStackTrace != null)
                    Console.WriteLine(info.ExceptionStackTrace);

                Console.ResetColor();
            }

            result = 1;
        }

        static void OnTestSkipped(TestSkippedInfo info)
        {
            lock (consoleLock)
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("[SKIP] {0}: {1}", info.TestDisplayName, info.SkipReason);
                Console.ResetColor();
            }
        }
    }
}