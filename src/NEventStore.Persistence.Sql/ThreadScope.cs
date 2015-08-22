namespace NEventStore.Persistence.Sql
{
    using System;
    using System.Threading;
    using System.Web;
    using NEventStore.Logging;
    using System.Threading.Tasks;
    using System.Runtime.Remoting.Messaging;

    public class ThreadScope<T> : IDisposable where T : class
    {
        //private readonly HttpContext _context = HttpContext.Current;
        private T _current;
        private readonly ILog _logger = LogFactory.BuildLogger(typeof (ThreadScope<T>));
        private bool _rootScope;
        private readonly string _threadKey;
        private bool _disposed;
        private readonly Func<Task<T>> _factory;

        public ThreadScope(string key, Func<Task<T>> factory)
        {
            _threadKey = typeof (ThreadScope<T>).Name + ":[{0}]".FormatWith(key ?? string.Empty);
            _factory = factory;
        }

        public async Task Initialize()
        {
            var parent = Load();
            _rootScope = parent == null;
            _logger.Debug(Messages.OpeningThreadScope, _threadKey, _rootScope);

            _current = parent ?? (await _factory());

            if (_current == null)
            {
                throw new ArgumentException(Messages.BadFactoryResult, "factory");
            }

            if (_rootScope)
            {
                Store(_current);
            }
        }

        public T Current
        {
            get { return _current; }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing || _disposed)
            {
                return;
            }

            _logger.Debug(Messages.DisposingThreadScope, _rootScope);
            _disposed = true;
            if (!_rootScope)
            {
                return;
            }

            _logger.Verbose(Messages.CleaningRootThreadScope);
            Store(null);

            var resource = _current as IDisposable;
            if (resource == null)
            {
                return;
            }

            _logger.Verbose(Messages.DisposingRootThreadScopeResources);
            resource.Dispose();
        }

        private T Load()
        {
            //if (_context != null)
            //{
            //    return _context.Items[_threadKey] as T;
            //}

            return CallContext.LogicalGetData(_threadKey) as T;
            //return Thread.GetData(Thread.GetNamedDataSlot(_threadKey)) as T;
        }

        private void Store(T value)
        {
            //if (_context != null)
            //{
            //    _context.Items[_threadKey] = value;
            //}
            //else
            {
                CallContext.LogicalSetData(_threadKey, value);
                //Thread.SetData(Thread.GetNamedDataSlot(_threadKey), value);
            }
        }
    }


    
}