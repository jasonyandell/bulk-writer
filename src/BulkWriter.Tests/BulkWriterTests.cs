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
        public void CanInitializeBulkCopyFromConstructor()
        {
            const int batchSize = 100;
            const int timeout = 90;

            var writer = new BulkWriter<BulkWriterTestsMyTestClass>(_connectionString,
                sbc =>
                {
                    sbc.BatchSize = batchSize;
                    sbc.BulkCopyTimeout = timeout;
                });

            Assert.Equal(batchSize, writer.BatchSize);
            Assert.Equal(timeout, writer.BulkCopyTimeout);
        }

    }
}