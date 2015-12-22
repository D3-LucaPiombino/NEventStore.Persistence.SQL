[assembly: Xunit.CollectionBehavior(DisableTestParallelization = true)]

namespace NEventStore.Persistence.AcceptanceTests
{
    using NEventStore.Persistence.Sql;
    using NEventStore.Persistence.Sql.SqlDialects;
    using NEventStore.Persistence.Sql.Tests;
    using NEventStore.Serialization;

    public partial class PersistenceEngineFixture
    {
        public PersistenceEngineFixture()
        {
            _createPersistence = (pageSize,systemTimeProvider) =>
                new SqlPersistenceFactory(new EnviromentConnectionFactory("MsSql", "System.Data.SqlClient"),
                    new BinarySerializer(),
                    new MsSqlDialect(),
                    pageSize: pageSize).Build();
        }
    }
}