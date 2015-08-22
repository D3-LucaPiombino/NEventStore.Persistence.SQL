namespace NEventStore.Persistence.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Globalization;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Transactions;
    using NEventStore.Logging;
    using NEventStore.Serialization;
    using ALinq;

    public class SqlPersistenceEngine : IPersistStreams
    {
        private static readonly ILog Logger = LogFactory.BuildLogger(typeof(SqlPersistenceEngine));
        private static readonly DateTime EpochTime = new DateTime(1970, 1, 1);
        private readonly IConnectionFactory _connectionFactory;
        private readonly ISqlDialect _dialect;
        private readonly int _pageSize;
        private readonly TransactionScopeOption _scopeOption;
        private readonly ISerialize _serializer;
        private bool _disposed;
        private int _initialized;
        private readonly IStreamIdHasher _streamIdHasher;

        public SqlPersistenceEngine(
            IConnectionFactory connectionFactory,
            ISqlDialect dialect,
            ISerialize serializer,
            TransactionScopeOption scopeOption,
            int pageSize)
            : this(connectionFactory, dialect, serializer, scopeOption, pageSize, new Sha1StreamIdHasher())
        { }

        public SqlPersistenceEngine(
            IConnectionFactory connectionFactory,
            ISqlDialect dialect,
            ISerialize serializer,
            TransactionScopeOption scopeOption,
            int pageSize,
            IStreamIdHasher streamIdHasher)
        {
            if (connectionFactory == null)
            {
                throw new ArgumentNullException("connectionFactory");
            }

            if (dialect == null)
            {
                throw new ArgumentNullException("dialect");
            }

            if (serializer == null)
            {
                throw new ArgumentNullException("serializer");
            }

            if (pageSize < 0)
            {
                throw new ArgumentException("pageSize");
            }

            if (streamIdHasher == null)
            {
                throw new ArgumentNullException("streamIdHasher");
            }

            _connectionFactory = connectionFactory;
            _dialect = dialect;
            _serializer = serializer;
            _scopeOption = scopeOption;
            _pageSize = pageSize;
            _streamIdHasher = new StreamIdHasherValidator(streamIdHasher);

            Logger.Debug(Messages.UsingScope, _scopeOption.ToString());
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public virtual async Task Initialize()
        {
            if (Interlocked.Increment(ref _initialized) > 1)
            {
                return;
            }

            Logger.Debug(Messages.InitializingStorage);
            await ExecuteCommand(statement => statement.ExecuteWithoutExceptions(_dialect.InitializeStorage));
        }

        public virtual IAsyncEnumerable<ICommit> GetFrom(string bucketId, string streamId, int minRevision, int maxRevision)
        {
            Logger.Debug(Messages.GettingAllCommitsBetween, streamId, minRevision, maxRevision);
            streamId = _streamIdHasher.GetHash(streamId);
            return ExecuteQuery(query =>
                {
                    string statement = _dialect.GetCommitsFromStartingRevision;
                    query.AddParameter(_dialect.BucketId, bucketId, DbType.AnsiString);
                    query.AddParameter(_dialect.StreamId, streamId, DbType.AnsiString);
                    query.AddParameter(_dialect.StreamRevision, minRevision);
                    query.AddParameter(_dialect.MaxStreamRevision, maxRevision);
                    query.AddParameter(_dialect.CommitSequence, 0);
                    return query
                        .ExecutePagedQuery(statement, _dialect.NextPageDelegate)
                        .Select(x => x.GetCommit(_serializer, _dialect));
                });
        }

        public virtual IAsyncEnumerable<ICommit> GetFrom(string bucketId, DateTime start)
        {
            start = start.AddTicks(-(start.Ticks % TimeSpan.TicksPerSecond)); // Rounds down to the nearest second.
            start = start < EpochTime ? EpochTime : start;

            Logger.Debug(Messages.GettingAllCommitsFrom, start, bucketId);
            return ExecuteQuery(query =>
                {
                    string statement = _dialect.GetCommitsFromInstant;
                    query.AddParameter(_dialect.BucketId, bucketId, DbType.AnsiString);
                    query.AddParameter(_dialect.CommitStamp, start);
                    return query.ExecutePagedQuery(statement, (q, r) => { })
                            .Select(x => x.GetCommit(_serializer, _dialect));

                });
        }

        public Task<ICheckpoint> GetCheckpoint(string checkpointToken)
        {
            if (string.IsNullOrWhiteSpace(checkpointToken))
            {
                return Task.FromResult<ICheckpoint>(new LongCheckpoint(-1));
            }
            return Task.FromResult<ICheckpoint>(LongCheckpoint.Parse(checkpointToken));
        }

        public virtual IAsyncEnumerable<ICommit> GetFromTo(string bucketId, DateTime start, DateTime end)
        {
            start = start.AddTicks(-(start.Ticks % TimeSpan.TicksPerSecond)); // Rounds down to the nearest second.
            start = start < EpochTime ? EpochTime : start;
            end = end < EpochTime ? EpochTime : end;

            Logger.Debug(Messages.GettingAllCommitsFromTo, start, end);
            return ExecuteQuery(query =>
                {
                    string statement = _dialect.GetCommitsFromToInstant;
                    query.AddParameter(_dialect.BucketId, bucketId, DbType.AnsiString);
                    query.AddParameter(_dialect.CommitStampStart, start);
                    query.AddParameter(_dialect.CommitStampEnd, end);
                    return query.ExecutePagedQuery(statement, (q, r) => { })
                        .Select(x => x.GetCommit(_serializer, _dialect));
                });
        }

        public virtual async Task<ICommit> Commit(CommitAttempt attempt)
        {
            ICommit commit;
            try
            {
                commit = await PersistCommit(attempt);
                Logger.Debug(Messages.CommitPersisted, attempt.CommitId);
            }
            catch (Exception e)
            {
                if (!(e is UniqueKeyViolationException))
                {
                    throw;
                }

                if (await DetectDuplicate(attempt))
                {
                    Logger.Info(Messages.DuplicateCommit);
                    throw new DuplicateCommitException(e.Message, e);
                }

                Logger.Info(Messages.ConcurrentWriteDetected);
                throw new ConcurrencyException(e.Message, e);
            }
            return commit;
        }

        public virtual IAsyncEnumerable<ICommit> GetUndispatchedCommits()
        {
            Logger.Debug(Messages.GettingUndispatchedCommits);
            return
                ExecuteQuery(query => 
                    query
                        .ExecutePagedQuery(_dialect.GetUndispatchedCommits, (q, r) => { }))
                        .Select(x => x.GetCommit(_serializer, _dialect)
                )
                .ToArray()
                .AsAsyncEnumerable(); // avoid paging
        }

        public virtual void MarkCommitAsDispatched(ICommit commit)
        {
            Logger.Debug(Messages.MarkingCommitAsDispatched, commit.CommitId);
            string streamId = _streamIdHasher.GetHash(commit.StreamId);
            ExecuteCommand(cmd =>
                {
                    cmd.AddParameter(_dialect.BucketId, commit.BucketId, DbType.AnsiString);
                    cmd.AddParameter(_dialect.StreamId, streamId, DbType.AnsiString);
                    cmd.AddParameter(_dialect.CommitSequence, commit.CommitSequence);
                    return cmd.ExecuteWithoutExceptions(_dialect.MarkCommitAsDispatched);
                });
        }

        public virtual IAsyncEnumerable<IStreamHead> GetStreamsToSnapshot(string bucketId, int maxThreshold)
        {
            Logger.Debug(Messages.GettingStreamsToSnapshot);
            return ExecuteQuery(query =>
                {
                    string statement = _dialect.GetStreamsRequiringSnapshots;
                    query.AddParameter(_dialect.BucketId, bucketId, DbType.AnsiString);
                    query.AddParameter(_dialect.Threshold, maxThreshold);
                    return query
                        .ExecutePagedQuery(
                            statement,
                            (q, s) => q.SetParameter(_dialect.StreamId, _dialect.CoalesceParameterValue(s.StreamId()), DbType.AnsiString)
                        )
                        .SelectSynch(x => x.GetStreamToSnapshot());
                });
        }

        public virtual Task<ISnapshot> GetSnapshot(string bucketId, string streamId, int maxRevision)
        {
            Logger.Debug(Messages.GettingRevision, streamId, maxRevision);
            var streamIdHash = _streamIdHasher.GetHash(streamId);
            return ExecuteQuery(query =>
                {
                    string statement = _dialect.GetSnapshot;
                    query.AddParameter(_dialect.BucketId, bucketId, DbType.AnsiString);
                    query.AddParameter(_dialect.StreamId, streamIdHash, DbType.AnsiString);
                    query.AddParameter(_dialect.StreamRevision, maxRevision);
                    return query
                        .ExecuteWithQuery(statement)
                        .SelectSynch(x => (ISnapshot)x.GetSnapshot(_serializer, streamId));
                })
                .FirstOrDefault();
        }

        public virtual async Task<bool> AddSnapshot(ISnapshot snapshot)
        {
            Logger.Debug(Messages.AddingSnapshot, snapshot.StreamId, snapshot.StreamRevision);
            string streamId = _streamIdHasher.GetHash(snapshot.StreamId);
            var result = await ExecuteCommand(async (connection, cmd) =>
            {
                cmd.AddParameter(_dialect.BucketId, snapshot.BucketId, DbType.AnsiString);
                cmd.AddParameter(_dialect.StreamId, streamId, DbType.AnsiString);
                cmd.AddParameter(_dialect.StreamRevision, snapshot.StreamRevision);
                _dialect.AddPayloadParamater(_connectionFactory, connection, cmd, _serializer.Serialize(snapshot.Payload));
                return await cmd.ExecuteWithoutExceptions(_dialect.AppendSnapshotToCommit);
            });
            return result > 0;
        }

        public virtual async Task Purge()
        {
            Logger.Warn(Messages.PurgingStorage);
            await ExecuteCommand(cmd => cmd.ExecuteNonQuery(_dialect.PurgeStorage));
        }

        public async Task Purge(string bucketId)
        {
            Logger.Warn(Messages.PurgingBucket, bucketId);
            await ExecuteCommand(cmd =>
                {
                    cmd.AddParameter(_dialect.BucketId, bucketId, DbType.AnsiString);
                    return cmd.ExecuteNonQuery(_dialect.PurgeBucket);
                });
        }

        public Task Drop()
        {
            Logger.Warn(Messages.DroppingTables);
            return ExecuteCommand(cmd => cmd.ExecuteNonQuery(_dialect.Drop));
        }

        public Task DeleteStream(string bucketId, string streamId)
        {
            Logger.Warn(Messages.DeletingStream, streamId, bucketId);
            streamId = _streamIdHasher.GetHash(streamId);
            return ExecuteCommand(cmd =>
                {
                    cmd.AddParameter(_dialect.BucketId, bucketId, DbType.AnsiString);
                    cmd.AddParameter(_dialect.StreamId, streamId, DbType.AnsiString);
                    return cmd.ExecuteNonQuery(_dialect.DeleteStream);
                });
        }

        public IAsyncEnumerable<ICommit> GetFrom(string bucketId, string checkpointToken)
        {
            LongCheckpoint checkpoint = LongCheckpoint.Parse(checkpointToken);
            Logger.Debug(Messages.GettingAllCommitsFromBucketAndCheckpoint, bucketId, checkpointToken);
            return ExecuteQuery(query =>
            {
                string statement = _dialect.GetCommitsFromBucketAndCheckpoint;
                query.AddParameter(_dialect.BucketId, bucketId, DbType.AnsiString);
                query.AddParameter(_dialect.CheckpointNumber, checkpoint.LongValue);
                return query.ExecutePagedQuery(statement, (q, r) => { })
                    .Select(x => x.GetCommit(_serializer, _dialect));
            });
        }

        public IAsyncEnumerable<ICommit> GetFrom(string checkpointToken)
        {
            LongCheckpoint checkpoint = LongCheckpoint.Parse(checkpointToken);
            Logger.Debug(Messages.GettingAllCommitsFromCheckpoint, checkpointToken);
            return ExecuteQuery(query =>
            {
                string statement = _dialect.GetCommitsFromCheckpoint;
                query.AddParameter(_dialect.CheckpointNumber, checkpoint.LongValue);
                return query.ExecutePagedQuery(statement, (q, r) => { })
                    .Select(x => x.GetCommit(_serializer, _dialect));
            });
        }

        public bool IsDisposed
        {
            get { return _disposed; }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing || _disposed)
            {
                return;
            }

            Logger.Debug(Messages.ShuttingDownPersistence);
            _disposed = true;
        }

        protected virtual void OnPersistCommit(IDbStatement cmd, CommitAttempt attempt)
        { }

        private Task<ICommit> PersistCommit(CommitAttempt attempt)
        {
            Logger.Debug(Messages.AttemptingToCommit, attempt.Events.Count, attempt.StreamId, attempt.CommitSequence, attempt.BucketId);
            string streamId = _streamIdHasher.GetHash(attempt.StreamId);
            return ExecuteCommand(async (connection, cmd) =>
            {
                cmd.AddParameter(_dialect.BucketId, attempt.BucketId, DbType.AnsiString);
                cmd.AddParameter(_dialect.StreamId, streamId, DbType.AnsiString);
                cmd.AddParameter(_dialect.StreamIdOriginal, attempt.StreamId);
                cmd.AddParameter(_dialect.StreamRevision, attempt.StreamRevision);
                cmd.AddParameter(_dialect.Items, attempt.Events.Count);
                cmd.AddParameter(_dialect.CommitId, attempt.CommitId);
                cmd.AddParameter(_dialect.CommitSequence, attempt.CommitSequence);
                cmd.AddParameter(_dialect.CommitStamp, attempt.CommitStamp);
                cmd.AddParameter(_dialect.Headers, _serializer.Serialize(attempt.Headers));
                _dialect.AddPayloadParamater(_connectionFactory, connection, cmd, _serializer.Serialize(attempt.Events.ToList()));
                OnPersistCommit(cmd, attempt);
                var checkpointNumber = (await cmd.ExecuteScalar(_dialect.PersistCommit)).ToLong();
                return (ICommit)new Commit(
                    attempt.BucketId,
                    attempt.StreamId,
                    attempt.StreamRevision,
                    attempt.CommitId,
                    attempt.CommitSequence,
                    attempt.CommitStamp,
                    checkpointNumber.ToString(CultureInfo.InvariantCulture),
                    attempt.Headers,
                    attempt.Events);
            });
        }

        private Task<bool> DetectDuplicate(CommitAttempt attempt)
        {
            string streamId = _streamIdHasher.GetHash(attempt.StreamId);
            return ExecuteCommand(async cmd =>
                {
                    cmd.AddParameter(_dialect.BucketId, attempt.BucketId, DbType.AnsiString);
                    cmd.AddParameter(_dialect.StreamId, streamId, DbType.AnsiString);
                    cmd.AddParameter(_dialect.CommitId, attempt.CommitId);
                    cmd.AddParameter(_dialect.CommitSequence, attempt.CommitSequence);
                    object value = await cmd.ExecuteScalar(_dialect.DuplicateCommit);
                    return (value is long ? (long)value : (int)value) > 0;
                });
        }
        

        protected virtual IAsyncEnumerable<T> ExecuteQuery<T>(Func<IDbStatement, IAsyncEnumerable<T>> query)
        {
            return ExecuteQuery(statement => Task.FromResult(query(statement)));
        }

        protected virtual IAsyncEnumerable<T> ExecuteQuery<T>(Func<IDbStatement, Task<IAsyncEnumerable<T>>> query)
        {
            
            return AsyncEnumerable.Create<T>(async producer =>
            {
                ThrowWhenDisposed();

                TransactionScope scope = OpenQueryScope();
                IDbAsyncConnection connection = null;
                IDbTransaction transaction = null;
                IDbStatement statement = null;
                try
                {
                    connection = await _connectionFactory.OpenAsync();
                    transaction = _dialect.OpenTransaction(connection);
                    statement = _dialect.BuildStatement(scope, connection, transaction);
                    statement.PageSize = _pageSize;

                    Logger.Verbose(Messages.ExecutingQuery);

                    await producer.Yield(await query(statement));

                    Logger.Verbose(Messages.ExecutingQuery);
                }
                catch (Exception e)
                {
                    if (statement != null)
                    {
                        statement.Dispose();
                    }
                    if (transaction != null)
                    {
                        transaction.Dispose();
                    }
                    if (connection != null)
                    {
                        connection.Dispose();
                    }
                    if (scope != null)
                    {
                        scope.Dispose();
                    }

                    Logger.Debug(Messages.StorageThrewException, e.GetType());
                    if (e is StorageUnavailableException)
                    {
                        throw;
                    }

                    throw new StorageException(e.Message, e);
                }
            });
        }

        protected virtual TransactionScope OpenQueryScope()
        {
            return OpenCommandScope() ?? new TransactionScope(TransactionScopeOption.Suppress);
        }

        private void ThrowWhenDisposed()
        {
            if (!_disposed)
            {
                return;
            }

            Logger.Warn(Messages.AlreadyDisposed);
            throw new ObjectDisposedException(Messages.AlreadyDisposed);
        }

        private Task<T> ExecuteCommand<T>(Func<IDbStatement, Task<T>> command)
        {
            return ExecuteCommand((_, statement) => command(statement));
        }

        protected virtual async Task<T> ExecuteCommand<T>(Func<IDbAsyncConnection, IDbStatement, Task<T>> command)
        {
            ThrowWhenDisposed();

            using (var scope = OpenCommandScope())
            using (var connection = await _connectionFactory.OpenAsync())
            using (var transaction = _dialect.OpenTransaction(connection))
            using (var statement = _dialect.BuildStatement(scope, connection, transaction))
            {
                try
                {
                    Logger.Verbose(Messages.ExecutingCommand);
                    T rowsAffected = await command(connection, statement);
                    Logger.Verbose(Messages.CommandExecuted, rowsAffected);

                    if (transaction != null)
                    {
                        transaction.Commit();
                    }

                    if (scope != null)
                    {
                        scope.Complete();
                    }

                    return rowsAffected;
                }
                catch (Exception e)
                {
                    Logger.Debug(Messages.StorageThrewException, e.GetType());
                    if (!RecoverableException(e))
                    {
                        throw new StorageException(e.Message, e);
                    }

                    Logger.Info(Messages.RecoverableExceptionCompletesScope);
                    if (scope != null)
                    {
                        scope.Complete();
                    }

                    throw;
                }
            }
        }

        protected virtual TransactionScope OpenCommandScope()
        {
            return new TransactionScope(_scopeOption, TransactionScopeAsyncFlowOption.Enabled);
        }

        private static bool RecoverableException(Exception e)
        {
            return e is UniqueKeyViolationException || e is StorageUnavailableException;
        }

       

        private class StreamIdHasherValidator : IStreamIdHasher
        {
            private readonly IStreamIdHasher _streamIdHasher;
            private const int MaxStreamIdHashLength = 40;

            public StreamIdHasherValidator(IStreamIdHasher streamIdHasher)
            {
                if (streamIdHasher == null)
                {
                    throw new ArgumentNullException("streamIdHasher");
                }
                _streamIdHasher = streamIdHasher;
            }
            public string GetHash(string streamId)
            {
                if (string.IsNullOrWhiteSpace(streamId))
                {
                    throw new ArgumentException(Messages.StreamIdIsNullEmptyOrWhiteSpace);
                }
                string streamIdHash = _streamIdHasher.GetHash(streamId);
                if (string.IsNullOrWhiteSpace(streamIdHash))
                {
                    throw new InvalidOperationException(Messages.StreamIdHashIsNullEmptyOrWhiteSpace);
                }
                if (streamIdHash.Length > MaxStreamIdHashLength)
                {
                    throw new InvalidOperationException(Messages.StreamIdHashTooLong.FormatWith(streamId, streamIdHash, streamIdHash.Length, MaxStreamIdHashLength));
                }
                return streamIdHash;
            }
        }
    }
}