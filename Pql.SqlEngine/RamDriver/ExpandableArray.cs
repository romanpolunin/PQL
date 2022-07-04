using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Pql.Engine.DataContainer.RamDriver
{
    internal class ExpandableArray<T>
    {
        /// <summary>
        /// For .NET 4.5, largest object size allowed on regular heap is 84999 bytes.
        /// Everything that is 85000 and above goes to large object heap.
        /// Also have to subtract 12 bytes for the (potentially expanded) object header.
        /// </summary>
        public const int LargestObjectSizeOnNormalHeap = 85000 - 1 - 12;

        private readonly object m_thisLock;
        private readonly int m_itemsPerBlock;
        private volatile int m_blockCount;
        private volatile T[][] m_list;
       
        public readonly int ElementsPerBlock;
        public int BlockCountIncrement { get { return 100; } }
        
        public ExpandableArray(int elementsPerItem, int itemByteSize)
        {
            if (elementsPerItem <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(elementsPerItem), elementsPerItem, "Elements per item must be positive");
            }

            m_thisLock = new object();
            m_list = null;
            m_itemsPerBlock = ComputeItemsPerBlock(itemByteSize);
            ElementsPerBlock = m_itemsPerBlock * elementsPerItem;
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
            var list = m_list;
            return list ?? new T[0][];
        }

        
        public int GetLocalIndex(int elementIndex)
        {
            return elementIndex % ElementsPerBlock;
        }

        public T this[int elementIndex]
        {
            
            get { return m_list[elementIndex / ElementsPerBlock][elementIndex % ElementsPerBlock]; }
            
            set { m_list[elementIndex / ElementsPerBlock][elementIndex % ElementsPerBlock] = value; }
        }

        
        public T[] GetBlock(int elementIndex)
        {
            return m_list[elementIndex / ElementsPerBlock];
        }

        public int Capacity
        {
            
            get { return m_blockCount * ElementsPerBlock; }
        }

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
            
            if (!System.Threading.Monitor.TryEnter(m_thisLock, timeout))
            {
                return false;
            }

            try
            {
                if (Capacity < capacity)
                {
                    var list = m_list;
                    var newBlockCount = 1 + capacity / ElementsPerBlock;

                    // do we have to reallocate list of blocks?
                    if (list == null || newBlockCount > list.Length)
                    {
                        // if yes, create a new one, copy block refs from old, replace pointer to list of blocks
                        var newList = new T[BlockCountIncrement + newBlockCount][];

                        int existing;
                        if (list != null)
                        {
                            existing = m_blockCount;
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
                            newList[i] = new T[m_itemsPerBlock];
                        }

                        Thread.MemoryBarrier();

                        m_list = newList;
                        m_blockCount = newBlockCount;
                    }
                    else if (m_blockCount < newBlockCount)
                    {
                        var existing = m_blockCount;
                        
                        // only allocate blocks for explicitly required capacity
                        for (var i = existing; i < newBlockCount; i++)
                        {
                            list[i] = new T[m_itemsPerBlock];
                        }

                        Thread.MemoryBarrier();

                        m_blockCount = newBlockCount;
                    }
                }
            }
            finally
            {
                System.Threading.Monitor.Exit(m_thisLock);
            }

            return true;
        }

        public void Clear()
        {
            lock (m_thisLock)
            {
                m_blockCount = 0;
            }
       }
    }
}