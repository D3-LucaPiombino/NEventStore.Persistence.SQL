namespace NEventStore.Persistence.Sql
{
    using ALinq;
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;

    public interface IDbAsyncConnection : IDbConnection
    {
        Task OpenAsync();

        IAsyncDbCommand CreateAsyncCommand();
    }


    public interface IAsyncDataReader : IDataReader
    {
        Task<bool> ReadAsync(CancellationToken cancellationToken = default (CancellationToken));
    }

    public interface IAsyncDbCommand : IDbCommand
    {
        Task<IAsyncDataReader> ExecuteReaderAsync();
        Task<int> ExecuteNonQueryAsync();

        Task<object> ExecuteScalarAsync();

    }

    class AsyncDataReader : IAsyncDataReader
    {
        private readonly DbDataReader _inner;
        public AsyncDataReader(DbDataReader inner)
        {
            _inner = inner;
        }


        public void Dispose()
        {
            ((IDisposable) _inner).Dispose();
        }

        public string GetName(int i)
        {
            return ((IDataRecord) _inner).GetName(i);
        }

        public string GetDataTypeName(int i)
        {
            return ((IDataRecord) _inner).GetDataTypeName(i);
        }

        public Type GetFieldType(int i)
        {
            return ((IDataRecord) _inner).GetFieldType(i);
        }

        public object GetValue(int i)
        {
            return ((IDataRecord) _inner).GetValue(i);
        }

        public int GetValues(object[] values)
        {
            return ((IDataRecord) _inner).GetValues(values);
        }

        public int GetOrdinal(string name)
        {
            return ((IDataRecord) _inner).GetOrdinal(name);
        }

        public bool GetBoolean(int i)
        {
            return ((IDataRecord) _inner).GetBoolean(i);
        }

        public byte GetByte(int i)
        {
            return ((IDataRecord) _inner).GetByte(i);
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            return ((IDataRecord) _inner).GetBytes(i, fieldOffset, buffer, bufferoffset, length);
        }

        public char GetChar(int i)
        {
            return ((IDataRecord) _inner).GetChar(i);
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            return ((IDataRecord) _inner).GetChars(i, fieldoffset, buffer, bufferoffset, length);
        }

        public Guid GetGuid(int i)
        {
            return ((IDataRecord) _inner).GetGuid(i);
        }

        public short GetInt16(int i)
        {
            return ((IDataRecord) _inner).GetInt16(i);
        }

        public int GetInt32(int i)
        {
            return ((IDataRecord) _inner).GetInt32(i);
        }

        public long GetInt64(int i)
        {
            return ((IDataRecord) _inner).GetInt64(i);
        }

        public float GetFloat(int i)
        {
            return ((IDataRecord) _inner).GetFloat(i);
        }

        public double GetDouble(int i)
        {
            return ((IDataRecord) _inner).GetDouble(i);
        }

        public string GetString(int i)
        {
            return ((IDataRecord) _inner).GetString(i);
        }

        public decimal GetDecimal(int i)
        {
            return ((IDataRecord) _inner).GetDecimal(i);
        }

        public DateTime GetDateTime(int i)
        {
            return ((IDataRecord) _inner).GetDateTime(i);
        }

        public IDataReader GetData(int i)
        {
            return ((IDataRecord) _inner).GetData(i);
        }

        public bool IsDBNull(int i)
        {
            return ((IDataRecord) _inner).IsDBNull(i);
        }

        public int FieldCount
        {
            get { return _inner.FieldCount; }
        }

        object IDataRecord.this[int i]
        {
            get { return _inner[i]; }
        }

        object IDataRecord.this[string name]
        {
            get { throw new NotImplementedException(); }
        }

        public void Close()
        {
            ((IDataReader) _inner).Close();
        }

        public DataTable GetSchemaTable()
        {
            return ((IDataReader) _inner).GetSchemaTable();
        }

        public bool NextResult()
        {
            return ((IDataReader) _inner).NextResult();
        }

        public bool Read()
        {
            return ((IDataReader) _inner).Read();
        }

        public int Depth
        {
            get { return _inner.Depth; }
        }

        public bool IsClosed
        {
            get { return _inner.IsClosed; }
        }

        public int RecordsAffected
        {
            get { return _inner.RecordsAffected; }
        }

        public Task<bool> ReadAsync(CancellationToken cancellationToken = default (CancellationToken))
        {
            return _inner.ReadAsync(cancellationToken);
        }
    }


    class AsyncDbCommand : IAsyncDbCommand
    {
        private readonly DbCommand _inner;
        public AsyncDbCommand(DbCommand inner)
        {
            _inner = inner;
        }

        

        public Task<int> ExecuteNonQueryAsync()
        {
            return _inner.ExecuteNonQueryAsync();
        }

        
        public async Task<IAsyncDataReader> ExecuteReaderAsync()
        {
            return new AsyncDataReader(await _inner.ExecuteReaderAsync());
        }

        
        public Task<object> ExecuteScalarAsync()
        {
            return _inner.ExecuteScalarAsync();
        }


        public void Dispose()
        {
            ((IDisposable) _inner).Dispose();
        }

        public void Prepare()
        {
            ((IDbCommand) _inner).Prepare();
        }

        public void Cancel()
        {
            ((IDbCommand) _inner).Cancel();
        }

        public IDbDataParameter CreateParameter()
        {
            return ((IDbCommand) _inner).CreateParameter();
        }

        public int ExecuteNonQuery()
        {
            return ((IDbCommand) _inner).ExecuteNonQuery();
        }

        public IDataReader ExecuteReader()
        {
            return ((IDbCommand) _inner).ExecuteReader();
        }

        public IDataReader ExecuteReader(CommandBehavior behavior)
        {
            return ((IDbCommand) _inner).ExecuteReader(behavior);
        }

        public object ExecuteScalar()
        {
            return ((IDbCommand) _inner).ExecuteScalar();
        }

        public IDbConnection Connection
        {
            get { return _inner.Connection; }
            set { ((IDbCommand)_inner).Connection = value; }
        }

        public IDbTransaction Transaction
        {
            get { return _inner.Transaction; }
            set { ((IDbCommand)_inner).Transaction = value; }
        }

        public string CommandText
        {
            get { return _inner.CommandText; }
            set { _inner.CommandText = value; }
        }

        public int CommandTimeout
        {
            get { return _inner.CommandTimeout; }
            set { _inner.CommandTimeout = value; }
        }

        public CommandType CommandType
        {
            get { return _inner.CommandType; }
            set { _inner.CommandType = value; }
        }

        public IDataParameterCollection Parameters
        {
            get { return _inner.Parameters; }
        }

        public UpdateRowSource UpdatedRowSource
        {
            get { return _inner.UpdatedRowSource; }
            set { _inner.UpdatedRowSource = value; }
        }
    }



    public class ConnectionScope : ThreadScope<DbConnection>, IDbAsyncConnection
    {
        public ConnectionScope(string connectionName, Func<Task<DbConnection>> factory)
            : base(connectionName, factory)
        {}

        IDbTransaction IDbConnection.BeginTransaction()
        {
            return Current.BeginTransaction();
        }

        IDbTransaction IDbConnection.BeginTransaction(IsolationLevel il)
        {
            return Current.BeginTransaction(il);
        }

        void IDbConnection.Close()
        {
            // no-op--let Dispose do the real work.
        }

        void IDbConnection.ChangeDatabase(string databaseName)
        {
            Current.ChangeDatabase(databaseName);
        }

        IDbCommand IDbConnection.CreateCommand()
        {
            return Current.CreateCommand();
        }

        public IAsyncDbCommand CreateAsyncCommand()
        {
            return new AsyncDbCommand(Current.CreateCommand());
        }

        void IDbConnection.Open()
        {
            Current.Open();
        }


        Task IDbAsyncConnection.OpenAsync()
        {
            return Current.OpenAsync();
        }

        string IDbConnection.ConnectionString
        {
            get { return Current.ConnectionString; }
            set { Current.ConnectionString = value; }
        }

        int IDbConnection.ConnectionTimeout
        {
            get { return Current.ConnectionTimeout; }
        }

        string IDbConnection.Database
        {
            get { return Current.Database; }
        }

        ConnectionState IDbConnection.State
        {
            get { return Current.State; }
        }
    }


    public static class AsyncEnumerableExtensions
    {
        public static Task Yield<T>(this ConcurrentAsyncProducer<T> producer, IAsyncEnumerable<T> other)
        {
            return other.ForEach(c => producer.Yield(c.Item));
        }
        public static IAsyncEnumerable<TOut> SelectSynch<TIn, TOut>(this IAsyncEnumerable<TIn> enumerable, Func<TIn, TOut> selector)
        {
            return AsyncEnumerable.Select(enumerable, item => Task.FromResult(selector(item)));
        }

        public static IAsyncEnumerable<T> AsAsyncEnumerable<T>(this Task<T[]> source)
        {
            return AsAsyncEnumerable(source.ContinueWith(p => (IEnumerable<T>)p.Result));
        }
        public static IAsyncEnumerable<T> AsAsyncEnumerable<T>(this Task<IEnumerable<T>> source)
        {
            return AsyncEnumerable.Create<T>(async producer =>
            {
                var enumerable = await source;
                foreach (var item in enumerable)
                {
                    await producer.Yield(item);
                }
            });
        }
    }
}