namespace Pql.SqlEngine.DataContainer.RamDriver
{
    internal class ExpandableArray<T>
    {
        /// <summary>
        /// For .NET 4.5, largest object size allowed on regular heap is 84999 bytes.
        /// Everything that is 85000 and above goes to large object heap.
        /// Also have to subtract 12 bytes for the (potentially expanded) object header.
        /// </summary>
        public const int LargestObjectSizeOnNormalHeap = 85000 - 1 - 12;

        private readonly object _thisLock;
        private readonly int _itemsPerBlock;
        private volatile int _blockCount;
        private volatile T[][]? _list;
       
        public readonly int ElementsPerBlock;
        public int BlockCountIncrement => 100;

        public ExpandableArray(int elementsPerItem, int itemByteSize)
        {
            if (elementsPerItem <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(elementsPerItem), elementsPerItem, "Elements per item must be positive");
            }

            _thisLock = new object();
            _list = null;
            _itemsPerBlock = ComputeItemsPerBlock(itemByteSize);
            ElementsPerBlock = _itemsPerBlock * elementsPerItem;
        }

        public static int ComputeItemsPerBlock(int itemByteSize)
        {
            // Number of elements per block must be product of 32 for compatibility with bitvector, because 32 = 8 * sizeof(int).
            // In addition to this, it should be small enough to prevent a block from going into large object heap.
            var result = ((LargestObjectSizeOnNormalHeap / itemByteSize) >> 5) << 5;
            return result > 0 ? result : 32;
        }

        
        public IEnumerable<T[]> EnumerateBlocks()
        {
            var list = _list;
            return list ?? Array.Empty<T[]>();
        }

        
        public int GetLocalIndex(int elementIndex)
        {
            return elementIndex % ElementsPerBlock;
        }

        public T this[int elementIndex]
        {

            get => _list[elementIndex / ElementsPerBlock][elementIndex % ElementsPerBlock];

            set => _list[elementIndex / ElementsPerBlock][elementIndex % ElementsPerBlock] = value;
        }


        public T[] GetBlock(int elementIndex)
        {
            return _list[elementIndex / ElementsPerBlock];
        }

        public int Capacity => _blockCount * ElementsPerBlock;

        /// <summary>
        /// Do not remove. Used implicitly from runtime code generator.
        /// </summary>
        public void EnsureCapacity(int capacity)
        {
            if (!TryEnsureCapacity(capacity, Timeout.Infinite))
            {
                throw new Exception("Failed");
            }
        }

        public bool TryEnsureCapacity(int capacity, int timeout = 0)
        {
            if (capacity < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(capacity), capacity, "New capacity value must be non-negative");
            }
            
            if (Capacity >= capacity)
            {
                return true;
            }
            
            if (!Monitor.TryEnter(_thisLock, timeout))
            {
                return false;
            }

            try
            {
                if (Capacity < capacity)
                {
                    var list = _list;
                    var newBlockCount = 1 + (capacity / ElementsPerBlock);

                    // do we have to reallocate list of blocks?
                    if (list == null || newBlockCount > list.Length)
                    {
                        // if yes, create a new one, copy block refs from old, replace pointer to list of blocks
                        var newList = new T[BlockCountIncrement + newBlockCount][];

                        int existing;
                        if (list != null)
                        {
                            existing = _blockCount;
                            for (var i = 0; i < existing; i++)
                            {
                                newList[i] = list[i];
                            }
                        }
                        else
                        {
                            existing = 0;
                        }

                        // only allocate blocks for explicitly required capacity
                        for (var i = existing; i < newBlockCount; i++)
                        {
                            newList[i] = new T[_itemsPerBlock];
                        }

                        Thread.MemoryBarrier();

                        _list = newList;
                        _blockCount = newBlockCount;
                    }
                    else if (_blockCount < newBlockCount)
                    {
                        var existing = _blockCount;
                        
                        // only allocate blocks for explicitly required capacity
                        for (var i = existing; i < newBlockCount; i++)
                        {
                            list[i] = new T[_itemsPerBlock];
                        }

                        Thread.MemoryBarrier();

                        _blockCount = newBlockCount;
                    }
                }
            }
            finally
            {
                Monitor.Exit(_thisLock);
            }

            return true;
        }

        public void Clear()
        {
            lock (_thisLock)
            {
                _blockCount = 0;
            }
       }
    }
}