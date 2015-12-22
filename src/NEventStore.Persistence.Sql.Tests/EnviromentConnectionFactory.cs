using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Threading.Tasks;

namespace NEventStore.Persistence.Sql.Tests
{
    public class EnviromentConnectionFactory : IConnectionFactory
    {
        private readonly string _envVarKey;
        private readonly DbProviderFactory _dbProviderFactory;

        public EnviromentConnectionFactory(string envDatabaseName, string providerInvariantName)
        {
            _envVarKey = string.Format("NEventStore.{0}", envDatabaseName);
            _dbProviderFactory = DbProviderFactories.GetFactory(providerInvariantName);
        }

        public async Task<IDbAsyncConnection> OpenAsync()
        {
            var scope = new ConnectionScope("master", OpenInternal);
            await scope.Initialize();
            return scope;
        }

        public Type GetDbProviderFactoryType()
        {
            return _dbProviderFactory.GetType();
        }

        private async Task<DbConnection> OpenInternal()
        {
            string connectionString = Environment.GetEnvironmentVariable(_envVarKey, EnvironmentVariableTarget.Process);
            if (connectionString == null)
                connectionString = "Data Source=(localdb)\\MSSQLLocalDB;Initial Catalog=NEventStoreUnitTests;Integrated Security=true;";
            if (connectionString == null)
            {
                string message =
                    string.Format(
                                  "Failed to get '{0}' environment variable. Please ensure " +
                                      "you have correctly setup the connection string environment variables. Refer to the " +
                                      "NEventStore wiki for details.",
                        _envVarKey);
                throw new InvalidOperationException(message);
            }
            connectionString = connectionString.TrimStart('"').TrimEnd('"');
            DbConnection connection = _dbProviderFactory.CreateConnection();
            Debug.Assert(connection != null, "connection != null");
            connection.ConnectionString = connectionString;
            try
            {
                await connection.OpenAsync();
            }
            catch (Exception e)
            {
                throw new StorageUnavailableException(e.Message, e);
            }
            return connection;
        }
    }
}