using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Pql.Engine.Interfaces;
using Pql.Engine.Interfaces.Internal;
using Pql.Engine.Interfaces.Services;

namespace Pql.Engine.DataContainer.RamDriver
{
    internal sealed class DocumentDataContainer : IDisposable
    {
        private readonly ITracer m_logger;
        private IUnmanagedAllocator m_allocator;
        private volatile int m_untrimmedDocumentCount;
        private volatile int m_capacity;
        private volatile bool m_stateBroken;

        public const int GrowthIncrement = 10000; 

        public readonly DocumentTypeDescriptor DocDesc;
        public readonly DataContainerDescriptor DataContainerDescriptor;
        public ConcurrentHashmapOfKeys DocumentIdToIndex;
        public readonly SortIndexManager SortIndexManager;
        public readonly ReaderWriterLockSlim StructureLock;
        
        /// <summary>
        /// All keys of documents, unordered. 
        /// Entries may be set to empty length for deleted documents.
        /// </summary>
        public ExpandableArrayOfKeys DocumentKeys;

        /// <summary>
        /// This is a bit vector where every bit corresponds to a document at some index.
        /// </summary>
        public BitVector ValidDocumentsBitmap;

        /// <summary>
        /// Every column's data is represented in memory by ColumnData{T}. 
        /// However we don't know data types at compile time, so references are stored as objects and casted later.
        /// This deals with in-RAM representation only, persistent store is a separate task.
        /// </summary>
        public readonly ColumnDataBase[] ColumnStores;

        /// <summary>
        /// Mapping from field ID to column store.
        /// </summary>
        public readonly Dictionary<int, int> FieldIdToColumnStore;

        /// <summary>
        /// Identifier of the field that is the primary key on this document.
        /// </summary>
        public readonly int PrimaryKeyFieldId;
        
        private string m_docRootPath;
        private bool m_disposed;

        public DocumentDataContainer(
            DataContainerDescriptor dataContainerDescriptor, 
            DocumentTypeDescriptor documentTypeDescriptor,
            IUnmanagedAllocator allocator,
            ITracer tracer)
        {
            m_logger = tracer ?? throw new ArgumentNullException(nameof(tracer));

            m_allocator = allocator ?? throw new ArgumentNullException(nameof(allocator));
            DocDesc = documentTypeDescriptor ?? throw new ArgumentNullException(nameof(documentTypeDescriptor));
            DataContainerDescriptor = dataContainerDescriptor ?? throw new ArgumentNullException(nameof(dataContainerDescriptor));

            ColumnStores = new ColumnDataBase[DocDesc.Fields.Length];
            DocumentKeys = new ExpandableArrayOfKeys(m_allocator);
            FieldIdToColumnStore = new Dictionary<int, int>(ColumnStores.Length * 2);
            PrimaryKeyFieldId = dataContainerDescriptor.RequireField(documentTypeDescriptor.DocumentType, documentTypeDescriptor.PrimaryKeyFieldName).FieldId;

            for (var i = 0; i < DocDesc.Fields.Length; i++)
            {
                var field = dataContainerDescriptor.RequireField(DocDesc.Fields[i]);
                ColumnStores[i] = CreateColumnStore(field.DbType, m_allocator, null);
                FieldIdToColumnStore.Add(field.FieldId, i);
            }

            DocumentIdToIndex = new ConcurrentHashmapOfKeys(m_allocator);
            ValidDocumentsBitmap = new BitVector(m_allocator);
            SortIndexManager = new SortIndexManager(this);
            StructureLock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        }

        private static ColumnDataBase CreateColumnStore(DbType dbType, IUnmanagedAllocator allocator, ColumnDataBase migrated)
        {
            var dataType = DriverRowData.DeriveSystemType(dbType);
            var columnStoreType = typeof(ColumnData<>).MakeGenericType(dataType);

            return migrated == null
                       ? (ColumnDataBase) Activator.CreateInstance(columnStoreType, dbType, allocator)
                       : (ColumnDataBase) Activator.CreateInstance(columnStoreType, migrated, allocator);
        }

        public IDriverDataEnumerator GetUnorderedEnumerator(
            IReadOnlyList<FieldMetadata> fields, int countOfMainFields, DriverRowData driverRow)
        {
            var untrimmedCount = m_untrimmedDocumentCount;
            if (untrimmedCount == 0)
            {
                return null;
            }
            
            return new DocumentDataContainerEnumerator_FullScan(untrimmedCount, driverRow, this, fields, countOfMainFields);
        }

        public IDriverDataEnumerator GetOrderedEnumerator(
            IReadOnlyList<FieldMetadata> fields, int countOfMainFields, DriverRowData driverRow, int orderFieldId, bool descending)
        {
            var untrimmedCount = m_untrimmedDocumentCount;
            if (untrimmedCount == 0)
            {
                return null;
            }

            var index = SortIndexManager.GetIndex(orderFieldId, m_untrimmedDocumentCount);
            return new DocumentDataContainerEnumerator_IndexScan(untrimmedCount, driverRow, this, fields, countOfMainFields, index, descending);
        }

        public IDriverDataEnumerator GetBulkUpdateEnumerator(List<FieldMetadata> fields, DriverRowData driverRow, IDriverDataEnumerator inputDataEnumerator)
        {
            var untrimmedCount = m_untrimmedDocumentCount;
            if (untrimmedCount == 0)
            {
                return null;
            }

            return new DocumentDataContainerEnumerator_BulkPkScan(untrimmedCount, driverRow, this, fields, inputDataEnumerator);
        }

        /// <summary>
        /// Will try to add a new document key and expand all column containers to make sure they can fit a new value.
        /// Returns false for duplicate keys or insufficient resources.
        /// </summary>
        public void TryAddDocument(byte[] key, out int index)
        {
            index = 0;
            if (DocumentIdToIndex.TryGetValueInt32(key, ref index))
            {
                OnUpdateValueDocumentsKeyIndex(index);
                return;
            }
            
            // make a copy of the key (so that we don't introduce dependency on caller's local variables) 
            // and reserve a new index value
            var newCount = Interlocked.Increment(ref m_untrimmedDocumentCount);
            if (newCount == int.MinValue)
            {
                // if we overflowed over max integer value, put max value back and throw
                Interlocked.CompareExchange(ref m_untrimmedDocumentCount, int.MaxValue, newCount);
                throw new Exception("Cannot expand storage any more");
            }

            index = newCount - 1;

            ExpandStorage(newCount);

            // now set values at the reserved index
            if (!DocumentKeys.TrySetAt(index, key))
            {
                throw new Exception("Failed to store new key value at " + index);
            }

            if (DocumentIdToIndex.TryAdd(DocumentKeys.GetIntPtrAt(index), index))
            {
                // mark document as valid, but don't touch any of its fields
                ValidDocumentsBitmap.SafeSet(index);
            }
            else
            {
                // seems like somebody slipped in and inserted the same value
                // mark our own generated index value as invalid
                ValidDocumentsBitmap.SafeClear(index);

                // now get "their" index and proceed to updating same record
                // some user data race is possible here, but container state won't be broken
                index = DocumentIdToIndex.GetInt32At(key);
            }
        }

        internal void AllocateAdditionalCapacity(int addCount) => ExpandStorage(m_capacity + addCount);

        internal void ExpandStorage(int newCount)
        {
            // There is no need to take additional locks (assuming that a read lock is already being held),
            // because of the way how every expandable array takes care of its expansion (e.g. grow-only, block-oriented).
            // Compaction might break this logic, but it will have to be executed while holding StructureLock in writable mode.
            // The only important global state is our capacity, which is a volatile variable.
            var currentCapacity = m_capacity;
            if (currentCapacity < newCount)
            {
                if (!StructureLock.IsReadLockHeld && !StructureLock.IsWriteLockHeld && !StructureLock.IsUpgradeableReadLockHeld)
                {
                    throw new InvalidOperationException("StructureLock must be held in any mode in order to start expansion");
                }

                var newCapacity = newCount - (newCount % GrowthIncrement) + GrowthIncrement;

                // first, expand field storage
                int goodCount;
                do
                {
                    goodCount = 0;

                    foreach (var store in ColumnStores)
                    {
                        if (m_capacity >= newCapacity)
                        {
                            // some other thread has completed the expansion
                            // stop whatever we are doing here
                            return;
                        }

                        // use "Try" variation here to allow multi-threaded allocation for blocks
                        if (store.TryEnsureCapacity(newCapacity))
                        {
                            goodCount++;
                        }
                    }

                    if (goodCount <= ColumnStores.Length / 2)
                    {
                        // seems like other threads are making progress on expansion too, 
                        // let them do something
                        Thread.Yield();
                    }
                } while (goodCount != ColumnStores.Length && m_capacity < newCapacity);

                // also expand backbone structures
                DocumentKeys.EnsureCapacity(newCapacity);
                ValidDocumentsBitmap.EnsureCapacity(newCapacity);

                // have to be careful here, multiple threads compete to set this value
                // some of those had new value for capacity larger or smaller than ours
                currentCapacity = m_capacity;
                while (currentCapacity < newCapacity)
                {
                    currentCapacity = Interlocked.CompareExchange(ref m_capacity, newCapacity, currentCapacity);
                }
            }
        }

        private void OnUpdateValueDocumentsKeyIndex(int index)
        {
            // does it point to a non-deleted entry?
            if (ValidDocumentsBitmap.SafeGetAndSet(index))
            {
                throw new Exception("Duplicate primary key for location " + index);
            }

            // make sure all fields are NULL,
            foreach (var store in ColumnStores)
            {
                store.NotNulls.SafeClear(index);
            }
        }

        /// <summary>
        /// Returns current number of uncompacted entries in the this document's registry.
        /// Is larger or equal to real documents count, and less or equal to capacity.
        /// </summary>
        public int UntrimmedCount { get { return m_untrimmedDocumentCount; } }


        
        public ColumnDataBase RequireColumnStore(int fieldId)
        {
            CheckState();

            var index = FieldIdToColumnStore[fieldId];
            ColumnStores[index].WaitLoadingCompleted();
            return ColumnStores[index];
        }

        
        public bool TryDeleteDocument(byte[] internalEntityId)
        {
            CheckState();

            int index = 0;
            if (DocumentIdToIndex.TryGetValueInt32(internalEntityId, ref index))
            {
                // mark document as deleted, if it is not yet marked as such
                if (ValidDocumentsBitmap.SafeGetAndClear(index))
                {
                    // mark all values as null so that they don't get read or written to disk
                    foreach (var colStore in ColumnStores)
                    {
                        colStore.NotNulls.SafeClear(index);
                    }

                    return true;
                }
            }

            return false;
        }

        public void FlushDataToStore(string docRootPath)
        {
            CheckState();

            if (docRootPath == null || !Directory.Exists(docRootPath))
            {
                throw new ArgumentException("Storage root is invalid: " + docRootPath);
            }

            StructureLock.EnterWriteLock();
            try
            {
                var tasks = new Task[2 + FieldIdToColumnStore.Count * 2];
                var count = 0;

                tasks[count] = new Task(
                    () =>
                        {
                            using var writer = new BinaryWriter(
                                new FileStream(
                                    Path.Combine(docRootPath, "_keysvalid.dat"),
                                    FileMode.Create, FileAccess.ReadWrite, FileShare.None, 1 << 22, FileOptions.None));
                            //WriteBitVectorToStore(writer, ValidDocumentsBitmap, m_untrimmedDocumentCount);
                            ValidDocumentsBitmap.Write(writer, (ulong)m_untrimmedDocumentCount);
                        }, TaskCreationOptions.LongRunning);

                count++;
                tasks[count] = tasks[count-1].ContinueWith(
                    prev =>
                        {
                            using var writer = new BinaryWriter(
                                new FileStream(
                                    Path.Combine(docRootPath, "_keys.dat"),
                                    FileMode.Create, FileAccess.ReadWrite, FileShare.None, 1 << 22, FileOptions.None));
                            DocumentKeys.Write(writer, (ulong)m_untrimmedDocumentCount, ValidDocumentsBitmap);
                        }, CancellationToken.None, TaskContinuationOptions.LongRunning, TaskScheduler.Default);

                count++;
                tasks[count - 2].Start();

                foreach (var pair in FieldIdToColumnStore)
                {
                    var field = DataContainerDescriptor.RequireField(pair.Key);
                    var colStore = ColumnStores[FieldIdToColumnStore[pair.Key]];

                    var colDataPath = Path.Combine(docRootPath, GetColumnDataFileName(field));
                    var colNotNullsPath = Path.Combine(docRootPath, GetColumnNotNullsFileName(field));

                    tasks[count] = new Task(
                        () =>
                            {
                                using var writer = new BinaryWriter(new FileStream(
                                        colNotNullsPath, FileMode.Create, FileAccess.ReadWrite, FileShare.None, 1 << 22, FileOptions.None));
                                colStore.NotNulls.Write(writer, (ulong)m_untrimmedDocumentCount);
                                writer.Flush();
                            }, TaskCreationOptions.LongRunning);

                    count++;
                    tasks[count] = tasks[count - 1].ContinueWith(
                        prev =>
                            {

                                using var writer = new BinaryWriter(new FileStream(
                                        colDataPath, FileMode.Create, FileAccess.ReadWrite, FileShare.None, 1 << 22, FileOptions.None));
                                colStore.WriteData(writer, m_untrimmedDocumentCount);
                                writer.Flush();
                            }, CancellationToken.None, TaskContinuationOptions.LongRunning, TaskScheduler.Default);

                    count++;
                    tasks[count - 2].Start();
                }

                Task.WaitAll(tasks);
            }
            finally
            {
                StructureLock.ExitWriteLock();
            }
        }

        private static string GetColumnDataFileName(FieldMetadata field) => string.Format("{0}-{1}-{2}.fdata", field.Name, field.FieldId, field.DbType);

        private static string GetColumnNotNullsFileName(FieldMetadata field) => string.Format("{0}-{1}-{2}.fnn", field.Name, field.FieldId, field.DbType);

        public void ReadDataFromStore(string docRootPath, int count)
        {
            CheckState();

            if (count > 0 && (docRootPath == null || !Directory.Exists(docRootPath)))
            {
                throw new ArgumentException("Count is non-zero, but storage root is invalid: " + docRootPath);
            }

            m_docRootPath = docRootPath;

            if (count < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(count), count, "Count cannot be negative");
            }

            m_untrimmedDocumentCount = count;
            m_capacity = count;

            DocumentIdToIndex.Clear();

            if (m_untrimmedDocumentCount == 0)
            {
                return;
            }

            var timer = Stopwatch.StartNew();

            using (var reader = new BinaryReader(new FileStream(Path.Combine(docRootPath, "_keysvalid.dat"),
                FileMode.Open, FileAccess.Read, FileShare.Read, 1 << 22, FileOptions.SequentialScan)))
            {
                //ReadBitVectorFromStore(reader, ValidDocumentsBitmap, m_untrimmedDocumentCount);
                ValidDocumentsBitmap.Read(reader, (ulong)m_untrimmedDocumentCount);
            }

            using (var reader = new BinaryReader(new FileStream(Path.Combine(docRootPath, "_keys.dat"),
                FileMode.Open, FileAccess.Read, FileShare.Read, 1 << 22, FileOptions.SequentialScan)))
            {
                DocumentKeys.Read(reader, (ulong)m_untrimmedDocumentCount, ValidDocumentsBitmap);

                for (var i = 0; i < m_untrimmedDocumentCount; i++)
                {
                    if (!DocumentIdToIndex.TryAdd(DocumentKeys.GetIntPtrAt(i), i))
                    {
                        throw new Exception("Failed to add the key at offset " + i + " to map");
                    }
                }
            }

            timer.Stop();

            if (m_logger.IsInfoEnabled)
            {
                m_logger.InfoFormat("Loaded container structure for document {0}/{1} in {2} milliseconds. {3} columns, {4} rows.", 
                    DocDesc.DocumentType, DocDesc.Name, timer.ElapsedMilliseconds, FieldIdToColumnStore.Count, m_untrimmedDocumentCount);
            }
        }

        private static void ReadBitVectorFromStore(BinaryReader reader, ExpandableArray<int> bitVectorData, int count)
        {
            var readSoFar = 0;
            bitVectorData.EnsureCapacity(count);
            const int elementsPerItem = 32;
            while (count > 0)
            {
                var block = bitVectorData.GetBlock(readSoFar);

                for (var i = 0; count > 0 && i < block.Length; i++, count -= elementsPerItem)
                {
                    block[i] = reader.ReadInt32();
                }

                readSoFar += bitVectorData.ElementsPerBlock;
            }
        }

        public void BeginLoadColumnStore(int fieldId)
        {
            CheckState();

            var colStore = ColumnStores[FieldIdToColumnStore[fieldId]];
            if (m_untrimmedDocumentCount == 0 || colStore.NotNulls.Capacity > 0 || colStore.IsLoadingInProgress)
            {
                // data structure already initialized, loading not needed
                return;
            }

            lock (colStore)
            {
                if (m_untrimmedDocumentCount == 0 || colStore.NotNulls.Capacity > 0 || colStore.IsLoadingInProgress)
                {
                    return;
                }

                var tasks = new Task[2];

                var field = DataContainerDescriptor.RequireField(fieldId);
                var colDataPath = Path.Combine(m_docRootPath, GetColumnDataFileName(field));
                var colNotNullsPath = Path.Combine(m_docRootPath, GetColumnNotNullsFileName(field));

                var readNotNulls = new Task(
                    () =>
                        {
                            using var reader = new BinaryReader(
                                new FileStream(
                                    colNotNullsPath, FileMode.Open, FileAccess.Read, FileShare.Read, 8 * 1024 * 1024, FileOptions.SequentialScan));
                            colStore.NotNulls.Read(reader, (ulong)m_untrimmedDocumentCount);
                        }, TaskCreationOptions.LongRunning);

                var readData = readNotNulls.ContinueWith(
                    x =>
                        {
                            using var reader = new BinaryReader(
                                new FileStream(
                                    colDataPath, FileMode.Open, FileAccess.Read, FileShare.Read, 8 * 1024 * 1024, FileOptions.SequentialScan));
                            colStore.ReadData(reader, m_untrimmedDocumentCount);
                        }, CancellationToken.None, TaskContinuationOptions.OnlyOnRanToCompletion | TaskContinuationOptions.LongRunning, TaskScheduler.Default);

                tasks[0] = readNotNulls;
                tasks[1] = readData;

                if (colStore.AttachLoaders(tasks))
                {
                    readNotNulls.Start();
                }
            }
        }

        public void Dispose() => Dispose(true);

        private void Dispose(bool disposing)
        {
            if (!m_disposed)
            {
                m_disposed = true;

                if (disposing)
                {
                    GC.SuppressFinalize(this);
                }

                foreach (var col in ColumnStores)
                {
                    col.Dispose();
                }

                ValidDocumentsBitmap.Dispose();
                DocumentIdToIndex.Dispose();
                DocumentKeys.Dispose();
            }
        }

        ~DocumentDataContainer()
        {
            Dispose(false);
        }

        public void MarkAsInvalid() => m_stateBroken = true;

        /// <summary>
        /// Reconstructs all unmanaged data in the new pool.
        /// Side effect is that all fragmentation is removed.
        /// </summary>
        /// <param name="newpool">The new memory pool to use.</param>
        public void MigrateRAM(IUnmanagedAllocator newpool)
        {
            CheckState();

            var vdb = ValidDocumentsBitmap;
            var dk = DocumentKeys;
            var diti = DocumentIdToIndex;
            var colstores = ColumnStores.ToArray();

            StructureLock.EnterWriteLock();
            try
            {
                // generate a new copy of the data
                var tasks = new List<Task>();

                tasks.Add(new Task<BitVector>(() => new BitVector(ValidDocumentsBitmap, newpool)));
                tasks.Add(new Task<ExpandableArrayOfKeys>(() => new ExpandableArrayOfKeys(DocumentKeys, newpool)));

                foreach (var c in ColumnStores)
                {
                    tasks.Add(new Task<ColumnDataBase>(o => CreateColumnStore(((ColumnDataBase)o).DbType, newpool, (ColumnDataBase)o), c));
                }

                foreach (var t in tasks)
                {
                    t.Start();
                }

                Task.WaitAll(tasks.ToArray());

                var newvdb = ((Task<BitVector>) tasks[0]).Result;
                var newdk = ((Task<ExpandableArrayOfKeys>) tasks[1]).Result;
                var newditi = new ConcurrentHashmapOfKeys(DocumentIdToIndex, newdk, newpool);
                
                // now, since no exception was thrown, let's consume results and dispose of old structures
                try
                {
                    ValidDocumentsBitmap = newvdb;
                    DocumentKeys = newdk;
                    DocumentIdToIndex = newditi;

                    for (var i = 2; i < tasks.Count; i++)
                    {
                        ColumnStores[i-2] = ((Task<ColumnDataBase>)tasks[i]).Result;
                    }
                    
                    vdb.Dispose();
                    dk.Dispose();
                    diti.Dispose();

                    foreach (var c in colstores)
                    {
                        c.Dispose();
                    }
                }
                catch
                {
                    m_stateBroken = true;
                    throw;
                }

                m_allocator = newpool;
            }
            finally
            {
                StructureLock.ExitWriteLock();
            }
        }

        public void CheckState()
        {
            if (m_stateBroken)
            {
                throw new InvalidOperationException("This DocumentDataContainer is marked broken because of previous failure");
            }
 
            if (m_disposed)
            {
                throw new ObjectDisposedException("DocumentDataContainer is disposed");
            }
        }
    }
}