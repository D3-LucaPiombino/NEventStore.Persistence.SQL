namespace NEventStore.Persistence.Sql.SqlDialects
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Transactions;
    using NEventStore.Logging;
    using NEventStore.Persistence.Sql;
    using System.Threading.Tasks;
    using ALinq;

    public class CommonDbStatement : IDbStatement
    {
        private const int InfinitePageSize = 0;
        private static readonly ILog Logger = LogFactory.BuildLogger(typeof (CommonDbStatement));
        private readonly IDbAsyncConnection _connection;
        private readonly ISqlDialect _dialect;
        private readonly TransactionScope _scope;
        private readonly IDbTransaction _transaction;

        public CommonDbStatement(
            ISqlDialect dialect,
            TransactionScope scope,
            IDbAsyncConnection connection,
            IDbTransaction transaction)
        {
            Parameters = new Dictionary<string, Tuple<object, DbType?>>();

            _dialect = dialect;
            _scope = scope;
            _connection = connection;
            _transaction = transaction;
        }

        protected IDictionary<string, Tuple<object, DbType?>> Parameters { get; private set; }

        protected ISqlDialect Dialect
        {
            get { return _dialect; }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public virtual int PageSize { get; set; }

        public virtual void AddParameter(string name, object value, DbType? parameterType = null)
        {
            Logger.Debug(Messages.AddingParameter, name);
            Parameters[name] = Tuple.Create(_dialect.CoalesceParameterValue(value), parameterType);
        }

        public virtual async Task<int> ExecuteWithoutExceptions(string commandText)
        {
            try
            {
                return await ExecuteNonQuery(commandText);
            }
            catch (Exception)
            {
                Logger.Debug(Messages.ExceptionSuppressed);
                return 0;
            }
        }

        public virtual async Task<int> ExecuteNonQuery(string commandText)
        {
            try
            {
                using (var command = BuildCommand(commandText))
                {
                    return await command.ExecuteNonQueryAsync();
                }
            }
            catch (Exception e)
            {
                if (_dialect.IsDuplicate(e))
                {
                    throw new UniqueKeyViolationException(e.Message, e);
                }

                throw;
            }
        }

        public virtual async Task<object> ExecuteScalar(string commandText)
        {
            try
            {
                using (var command = BuildCommand(commandText))
                {
                    return await command.ExecuteScalarAsync().ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                if (_dialect.IsDuplicate(e))
                {
                    throw new UniqueKeyViolationException(e.Message, e);
                }
                throw;
            }
        }

        public virtual IAsyncEnumerable<IDataRecord> ExecuteWithQuery(string queryText)
        {
            return ExecuteQuery(queryText, (query, latest) => { }, InfinitePageSize);
        }

        public virtual IAsyncEnumerable<IDataRecord> ExecutePagedQuery(string queryText, NextPageDelegate nextpage)
        {
            int pageSize = _dialect.CanPage ? PageSize : InfinitePageSize;
            if (pageSize > 0)
            {
                Logger.Verbose(Messages.MaxPageSize, pageSize);
                Parameters.Add(_dialect.Limit, Tuple.Create((object) pageSize, (DbType?) null));
            }

            return ExecuteQuery(queryText, nextpage, pageSize);
        }

        protected virtual void Dispose(bool disposing)
        {
            Logger.Verbose(Messages.DisposingStatement);

            if (_transaction != null)
            {
                _transaction.Dispose();
            }

            if (_connection != null)
            {
                _connection.Dispose();
            }

            if (_scope != null)
            {
                _scope.Dispose();
            }
        }

        protected virtual IAsyncEnumerable<IDataRecord> ExecuteQuery2(string queryText, NextPageDelegate nextpage, int pageSize)
        {
            Parameters.Add(_dialect.Skip, Tuple.Create((object) 0, (DbType?) null));
            var command = BuildCommand(queryText);

            try
            {
                return new AsyncPagedEnumerationCollection(_scope, _dialect, command, nextpage, pageSize, this);
            }
            catch (Exception)
            {
                command.Dispose();
                throw;
            }
        }

        protected virtual IAsyncEnumerable<IDataRecord> ExecuteQuery(string queryText, NextPageDelegate nextpage, int pageSize)
        {
            Parameters.Add(_dialect.Skip, Tuple.Create((object)0, (DbType?)null));
            return AsyncEnumerable.Create<IDataRecord>(async producer =>
            {
                using (var command = BuildCommand(queryText))
                {
                    var position = 0;
                    var read = 0;
                    try
                    { 
                        do
                        {
                            using (var reader = await command.ExecuteReaderAsync())
                            {
                                read = 0;
                                while (await reader.ReadAsync())
                                {
                                    await producer.Yield(reader);
                                    position++;
                                    read++;

                                    if (IsPageCompletelyEnumerated(pageSize, position))
                                    {
                                        command.SetParameter(_dialect.Skip, position);
                                        nextpage(command, reader);
                                    }
                                }
                                Logger.Verbose(Messages.EnumeratedRowCount, position);
                            }
                        }
                        while (read > 0 && IsPageCompletelyEnumerated(pageSize,position));
                        
                    }
                    catch (Exception e)
                    {
                        Logger.Debug(Messages.EnumerationThrewException, e.GetType());
                        throw new StorageUnavailableException(e.Message, e);
                    }
                }
            });
            
        }

        private bool IsPageCompletelyEnumerated(int pageSize, int position)
        {
            return pageSize > 0 &&  // Check if paging is disabled (pageSize == 0)
                (position > 0 && 0 == (position % pageSize)); // Check if we have enumerated all the rows in this page
        }


        protected virtual IAsyncDbCommand BuildCommand(string statement)
        {
            Logger.Verbose(Messages.CreatingCommand);
            var command = _connection.CreateAsyncCommand();

            int timeout = 0;
            if( int.TryParse( System.Configuration.ConfigurationManager.AppSettings["NEventStore.SqlCommand.Timeout"], out timeout ) ) 
            {
              command.CommandTimeout = timeout;
            }

            command.Transaction = _transaction;
            command.CommandText = statement;

            Logger.Verbose(Messages.ClientControlledTransaction, _transaction != null);
            Logger.Verbose(Messages.CommandTextToExecute, statement);

            BuildParameters(command);

            return command;
        }

        protected virtual void BuildParameters(IDbCommand command)
        {
            foreach (var item in Parameters)
            {
                BuildParameter(command, item.Key, item.Value.Item1, item.Value.Item2);
            }
        }

        protected virtual void BuildParameter(IDbCommand command, string name, object value, DbType? dbType)
        {
            IDbDataParameter parameter = command.CreateParameter();
            parameter.ParameterName = name;
            SetParameterValue(parameter, value, dbType);

            Logger.Verbose(Messages.BindingParameter, name, parameter.Value);
            command.Parameters.Add(parameter);
        }

        protected virtual void SetParameterValue(IDataParameter param, object value, DbType? type)
        {
            param.Value = value ?? DBNull.Value;
            param.DbType = type ?? (value == null ? DbType.Binary : param.DbType);
        }
    }

}