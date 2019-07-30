using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Pql.ExpressionEngine.Utilities
{
    /// <summary>
    /// Implements object pool.
    /// </summary>
    /// <typeparam name="T">Type of object to hold</typeparam>
    internal class ObjectPool<T> where T : class
    {
        private readonly Func<T> m_objectFactory;
        private readonly BlockingCollection<T> m_items;

        /// <summary>
        /// Ctr.
        /// </summary>
        /// <param name="boundedCapacity">Maximum number of objects to hold</param>
        /// <param name="objectFactory">Optional factory function to produce new objects</param>
        public ObjectPool(int boundedCapacity, Func<T> objectFactory)
        {
            if (boundedCapacity <= 0)
            {
                throw new ArgumentOutOfRangeException("boundedCapacity", boundedCapacity, "Capacity cap must be positive");
            }

            m_objectFactory = objectFactory;
            m_items = new BlockingCollection<T>(boundedCapacity);
        }

        /// <summary>
        /// Maximum number of objects to be held.
        /// </summary>
        public int Capacity
        {
            get { return m_items.BoundedCapacity; }
        }

        /// <summary>
        /// Attempts to take an available object from pool.
        /// Will wait until cancellation token is signaled if pool is empty.
        /// </summary>
        /// <returns>Helper to facilitate guaranteed return of the object to the pool</returns>
        public ObjectPoolAccessor Take(CancellationToken cancellation)
        {
            if (m_objectFactory == null)
            {
                return new ObjectPoolAccessor(this, m_items.Take(cancellation));
            }

            if (!m_items.TryTake(out var item, 0, cancellation))
            {
                item = m_objectFactory();
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
            if (!m_items.TryAdd(item) && m_objectFactory == null)
            {
                throw new InvalidOperationException("Could not return an item to the pool. Capacity must have been exceeded");
            }
        }

        /// <summary>
        /// Helper to facilitate IDisposable pattern.
        /// </summary>
        public class ObjectPoolAccessor : IDisposable
        {
            private readonly ObjectPool<T> m_pool;
            private T m_item;

            /// <summary>
            /// Element to be returned to the pool once this holder is disposed.
            /// </summary>
            public T Item { get { return m_item; } }

            /// <summary>
            /// Ctr.
            /// </summary>
            public ObjectPoolAccessor(ObjectPool<T> pool, T item)
            {
                m_pool = pool ?? throw new ArgumentNullException("pool");
                m_item = item ?? throw new ArgumentNullException("item");
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

                var item = Interlocked.CompareExchange(ref m_item, null, m_item);
                if (item != null)
                {
                    m_pool.Return(item);
                }
            }
        }
    }
}