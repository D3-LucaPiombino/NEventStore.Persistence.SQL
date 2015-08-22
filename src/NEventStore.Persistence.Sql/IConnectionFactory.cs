namespace NEventStore.Persistence.Sql
{
    using System;
    using System.Data;
    using System.Threading.Tasks;

    public interface IConnectionFactory
    {
        Task<IDbAsyncConnection> OpenAsync();

        Type GetDbProviderFactoryType();
    }
}