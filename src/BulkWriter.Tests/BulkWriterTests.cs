using System;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace BulkWriter.Tests
{
    public class BulkWriterTests
    {
        private readonly string _connectionString = TestHelpers.ConnectionString;
        private readonly string _tableName = nameof(BulkWriterTestsMyTestClass);

        public class BulkWriterTestsMyTestClass
        {
            public int Id { get; set; }

            public string Name { get; set; }
        }

        public BulkWriterTests()
        {
            TestHelpers.ExecuteNonQuery(_connectionString, $"DROP TABLE IF EXISTS [dbo].[{_tableName}]");

            TestHelpers.ExecuteNonQuery(_connectionString,
                "CREATE TABLE [dbo].[" + _tableName + "](" +
                "[Id] [int] IDENTITY(1,1) NOT NULL," +
                "[Name] [nvarchar](50) NULL," +
                "CONSTRAINT [PK_" + _tableName + "] PRIMARY KEY CLUSTERED ([Id] ASC)" +
                ")");
        }

        [Fact]
        public async Task CanWriteSync()
        {
            var writer = new BulkWriter<BulkWriterTestsMyTestClass>(_connectionString);

            var items = Enumerable.Range(1, 1000).Select(i => new BulkWriterTestsMyTestClass { Id = i, Name = "Bob"});

            writer.WriteToDatabase(items);

            var count = (int) await TestHelpers.ExecuteScalar(_connectionString, $"SELECT COUNT(1) FROM {_tableName}");

            Assert.Equal(1000, count);
        }

        [Fact]
        public void CanSetBulkCopyParameters()
        {
            const int timeout = 10;
            const int batchSize = 1000;

            BulkWriter<BulkWriterTestsMyTestClass> writer = null;
            try
            {
                writer = new BulkWriter<BulkWriterTestsMyTestClass>(_connectionString)
                {
                    BulkCopyTimeout = timeout,
                    BatchSize = batchSize,
                    BulkCopySetup = bcp =>
                    {
                        Assert.Equal(timeout, bcp.BulkCopyTimeout);
                        Assert.Equal(batchSize, bcp.BatchSize);
                    }
                };

                var items = Enumerable.Range(1, 10).Select(i => new BulkWriterTestsMyTestClass { Id = i, Name = "Bob" });

                writer.WriteToDatabase(items);
            }
            finally
            {
                writer?.Dispose();
            }            
        }
    }
}