using NEventStore.Persistence.Sql.Tests;

namespace NEventStore.Persistence.AcceptanceTests
{
    using System;
    using NEventStore.Persistence.AcceptanceTests.BDD;
    using NEventStore.Persistence.Sql;
    using NEventStore.Persistence.Sql.SqlDialects;
    using Xunit;
    using System.Threading.Tasks;
    using FluentAssertions;

    public class when_specifying_a_hasher : SpecificationBase
    {
        private bool _hasherInvoked;
        private IStoreEvents _eventStore;

        protected override Task Context()
        {
            _eventStore = Wireup
                .Init()
                .UsingSqlPersistence(new EnviromentConnectionFactory("MsSql", "System.Data.SqlClient"))
                .WithDialect(new MsSqlDialect())
                .WithStreamIdHasher(streamId =>
                {
                    _hasherInvoked = true;
                    return new Sha1StreamIdHasher().GetHash(streamId);
                })
                .InitializeStorageEngine()
                .EnlistInAmbientTransaction()
                .UsingBinarySerialization()
                .Build();

            return Task.FromResult(false);
        }

        protected override async Task Cleanup()
        {
            if (_eventStore != null)
            {
                await _eventStore.Advanced.Drop();
                _eventStore.Dispose();
            }
        }

        protected override async Task Because()
        {
            using (var stream = await _eventStore.OpenStream(Guid.NewGuid()))
            {
                stream.Add(new EventMessage{ Body = "Message" });
                await stream.CommitChanges(Guid.NewGuid());
            }
        }

        [Fact]
        public void should_invoke_hasher()
        {
            _hasherInvoked.Should().BeTrue();
        }
    }


    //public class when_having_high_concurrency : SpecificationBase
    //{
    //    private IStoreEvents _eventStore;

    //    protected override Task Context()
    //    {
    //        _eventStore = Wireup
    //            .Init()
    //            .UsingSqlPersistence(new EnviromentConnectionFactory("MsSql", "System.Data.SqlClient"))
    //            .WithDialect(new MsSqlDialect())
    //            .InitializeStorageEngine()
                
    //            //.EnlistInAmbientTransaction()
    //            .UsingBinarySerialization()
    //            .Build();

    //        return Task.FromResult(false);
    //    }

    //    protected override void Cleanup()
    //    {
    //        if (_eventStore != null)
    //        {
    //            _eventStore.Advanced.Drop().Wait();
    //            _eventStore.Dispose();
    //        }
    //    }

    //    protected override async Task Because()
    //    {
            
    //    }

    //    [Fact]
    //    public async Task should_invoke_hasher()
    //    {
    //        using (var reader = File.OpenText(@"D:\MrJingle\Dev\ironruby-1.1-dotnet3.5.zip"))
    //        {

    //            var ev = new EventMessage { Body = await reader.ReadToEndAsync() };
    //            for (int i = 0; i < 1; i++)
    //            {
    //                var tasks = Enumerable.Range(0, 1)
    //                    .Select(_ => Task.Run(() => Do(ev)));
    //                await Task.WhenAll(tasks);
    //            }
                
    //        }
    //    }

    //    private async Task Do(EventMessage ev)
    //    {
    //        using (var stream = await _eventStore.OpenStream(Guid.NewGuid()))
    //        {
    //            stream.Add(ev);
    //            await stream.CommitChanges(Guid.NewGuid());
    //        }
    //    }
    //}
}