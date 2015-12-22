using NEventStore.Persistence.Sql.Tests;

[assembly: Xunit.CollectionBehavior(DisableTestParallelization = true)]

namespace NEventStore.Persistence.AcceptanceTests
{
    using NEventStore.Persistence.Sql;
    using NEventStore.Persistence.Sql.SqlDialects;
    using NEventStore.Serialization;

    public partial class PersistenceEngineFixture
    {
        public PersistenceEngineFixture()
        {
            _createPersistence = (pageSize, systemTimeProvider) =>
                new SqlPersistenceFactory(
                    new EnviromentConnectionFactory("Oracle", "Oracle.ManagedDataAccess.Client"),
                    new BinarySerializer(),
                    new OracleNativeDialect(),
                    pageSize: pageSize).Build();
        }
    }
}