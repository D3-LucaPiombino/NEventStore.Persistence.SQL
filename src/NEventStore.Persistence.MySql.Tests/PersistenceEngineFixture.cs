﻿using NEventStore.Persistence.Sql.Tests;

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
            _createPersistence = (pageSize,systemTimeProvider) =>
                    new SqlPersistenceFactory(
                        new EnviromentConnectionFactory("MySql", "MySql.Data.MySqlClient"),
                        new BinarySerializer(),
                        new MySqlDialect(),
                        pageSize: pageSize).Build();

            Wireup.Init().UsingSqlPersistence("test");
        }
    }
}