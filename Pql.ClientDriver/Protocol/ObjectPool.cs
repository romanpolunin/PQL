using System.Collections.Concurrent;

namespace Pql.ClientDriver.Protocol
{
    /// <summary>
    /// Implements object pool with bounded capacity.
    /// </summary>
    /// <typeparam name="T">Type of object to hold</typeparam>
    public class ObjectPool<T> where T: class
    {
        private readonly Func<T> _objectFactory;
        private readonly BlockingCollection<T> _items;

        /// <summary>
        /// Ctr.
        /// </summary>
        /// <param name="boundedCapacity">Maximum number of objects to hold</param>
        /// <param name="objectFactory">Optional factory function to produce new objects</param>
        public ObjectPool(int boundedCapacity, Func<T> objectFactory)
        {
            if (boundedCapacity <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(boundedCapacity), boundedCapacity, "Capacity cap must be positive");
            }

            _objectFactory = objectFactory;
            _items = new BlockingCollection<T>(boundedCapacity);
        }

        /// <summary>
        /// Maximum number of objects to be held.
        /// </summary>
        public int Capacity => _items.BoundedCapacity;

        /// <summary>
        /// Attempts to take an available object from pool.
        /// Will wait until cancellation token is signaled if pool is empty.
        /// </summary>
        /// <returns>Helper to facilitate guaranteed return of the object to the pool</returns>
        public ObjectPoolAccessor Take(CancellationToken cancellation)
        {
            if (_objectFactory == null)
            {
                return new ObjectPoolAccessor(this, _items.Take(cancellation));
            }

            if (!_items.TryTake(out var item, 0, cancellation))
            {
                item = _objectFactory();
            }

            return new ObjectPoolAccessor(this, item);
        }

        /// <summary>
        /// Returns an object to the pool. 
        /// Also use this method to initially populate the pool with free objects.
        /// </summary>
        /// <exception cref="Exception">Bounded capacity exceeded</exception>
        public void Return(T item)
        {
            if (!_items.TryAdd(item) && _objectFactory == null)
            {
                throw new InvalidOperationException("Could not return an item to the pool. Capacity must have been exceeded");
            }
        }

        /// <summary>
        /// Helper to facilitate IDisposable approach for returning objects to the parent pool.
        /// </summary>
        public class ObjectPoolAccessor : IDisposable
        {
            private readonly ObjectPool<T> _pool;
            private T? _item;

            /// <summary>
            /// Element to be returned to the pool once this holder is disposed.
            /// </summary>
            public T? Item => _item ?? throw new ObjectDisposedException("item");

            /// <summary>
            /// Ctr.
            /// </summary>
            public ObjectPoolAccessor(ObjectPool<T> pool, T item)
            {
                _pool = pool ?? throw new ArgumentNullException(nameof(pool));
                _item = item ?? throw new ArgumentNullException(nameof(item));
            }

            /// <summary>
            /// Dispose returns the object to pool.
            /// </summary>
            public void Dispose()
            {
                if (Environment.HasShutdownStarted)
                {
                    // nobody cares now
                    return;
                }

                var item = Interlocked.CompareExchange(ref _item, null, _item);
                if (item != null)
                {
                    _pool.Return(item);
                }
            }
        }
    }
}