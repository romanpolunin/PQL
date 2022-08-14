using System.Collections.Concurrent;

namespace Pql.SqlEngine.Interfaces.Internal
{
    /// <summary>
    /// A combination of a blocking collection and a pool. 
    /// Helps reduce dynamic memory allocation by providing means for workers to reuse data buffers.
    /// </summary>
    /// <typeparam name="T">Type of the work item object.</typeparam>
    public class DataRing<T> : IDisposable where T : class 
    {
        private BlockingCollection<T> _readyForProcessing;
        private BlockingCollection<T> _completedProcessing;
        private volatile bool _disposed;

        /// <summary>
        /// Ctr.
        /// </summary>
        /// <param name="maxQueueSize">Maximum number of items to be enqueued at any time, affects <see cref="AddTaskForProcessing"/>.</param>
        /// <param name="maxPoolSize">Maximum number of items to reside in the pool of fresh items, affects <see cref="TakeCompletedTask"/> and <see cref="ReturnCompletedTask"/>.</param>
        public DataRing(int maxQueueSize, int maxPoolSize)
        {
            if (maxQueueSize <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxQueueSize), maxQueueSize, "Must be positive");
            }

            if (maxPoolSize <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxPoolSize), maxPoolSize, "Must be positive");
            }

            _readyForProcessing = new BlockingCollection<T>(new ConcurrentQueue<T>(), maxQueueSize);
            _completedProcessing = new BlockingCollection<T>(new ConcurrentQueue<T>(), maxPoolSize);
        }

        /// <summary>
        /// Number of items in the queue, affects clients of <see cref="ConsumeProcessingTasks"/> and <see cref="AddTaskForProcessing"/>.
        /// Cannot be more than the constraint value provided in the constructor.
        /// </summary>
        public int QueueLength => _disposed ? 0 : _readyForProcessing.Count;

        /// <summary>
        /// Takes an object from the pool or blocks until some object is returned to pool.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token to observe.</param>
        public T TakeCompletedTask(CancellationToken cancellationToken)
        {
            CheckDisposed();
            var item = _completedProcessing.Take(cancellationToken);
            return item;
        }

        /// <summary>
        /// Puts object into the processing queue. May block if the queue is full.
        /// </summary>
        /// <param name="item">Item to be enqueued.</param>
        /// <param name="cancellationToken">Cancellation token to observe.</param>
        public void AddTaskForProcessing(T item, CancellationToken cancellationToken)
        {
            CheckDisposed();
            _readyForProcessing.Add(item, cancellationToken);
        }

        /// <summary>
        /// Returns an object to the pool. 
        /// Assumption is that it was previously retrieved from <see cref="ConsumeProcessingTasks"/>.
        /// </summary>
        public void ReturnCompletedTask(T item)
        {
            CheckDisposed();
            if (!_completedProcessing.TryAdd(item, 0))
            {
                throw new InvalidOperationException("Internal error: pool size cannot grow above " + _completedProcessing.BoundedCapacity);
            }
        }

        /// <summary>
        /// Blocking enumerable for consumers. Retrieved objects should be returned to the pool using <see cref="ReturnCompletedTask"/>.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token to observe.</param>
        public IEnumerable<T> ConsumeProcessingTasks(CancellationToken cancellationToken)
        {
            CheckDisposed();
            return _readyForProcessing.GetConsumingEnumerable(cancellationToken);
        }

        /// <summary>
        /// To be called by the monitor when there are no more items to be added using <see cref="AddTaskForProcessing"/>, 
        /// to inform consumers that they should stop waiting for more.
        /// </summary>
        public void CompleteAddingTasksForProcessing()
        {
            CheckDisposed();
            
            var ready = _readyForProcessing;
            if (ready != null && !ready.IsAddingCompleted)
            {
                ready.CompleteAdding();
            }
        }

        /// <summary>
        /// To be called by the monitor when there are no more items to be added using <see cref="AddTaskForProcessing"/>, 
        /// to inform consumers that they should stop waiting for more.
        /// </summary>
        public void CompleteAddingCompletedTasks()
        {
            CheckDisposed();

            var completed = _completedProcessing;
            if (completed != null && !completed.IsAddingCompleted)
            {
                completed.CompleteAdding();
            }
        }

        private void CheckDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(GetType().Name);
            }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose() => Dispose(true);

        private void Dispose(bool disposing)
        {
            if (disposing)
            {
                GC.SuppressFinalize(this);
            }

            _disposed = true;

            var ready = Interlocked.CompareExchange(ref _readyForProcessing, null, _readyForProcessing);
            var completed = Interlocked.CompareExchange(ref _completedProcessing, null, _completedProcessing);
            
            if (ready != null && !ready.IsCompleted)
            {
                while (ready.TryTake(out _))
                {
                    
                }

                ready.Dispose();
            }

            if (completed != null && !completed.IsCompleted)
            {
                while (completed.TryTake(out _))
                {
                    
                }

                completed.Dispose();
            }
        }
    }
}
