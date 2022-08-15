using System.Text.Json;

using Pql.SqlEngine.Interfaces;
using Pql.SqlEngine.Interfaces.Internal;
using Pql.SqlEngine.Interfaces.Services;
using Pql.UnmanagedLib;

namespace Pql.SqlEngine.DataContainer.RamDriver
{
    internal sealed class DataContainer : IDisposable
    {
        private readonly ITracer _tracer;
        private volatile IUnmanagedAllocator _memoryPool;
        private readonly DataContainerDescriptor _dataContainerDescriptor;
        private readonly string _storageRoot;
        private readonly Dictionary<int, DocumentDataContainer> _documentDataContainers;
        private readonly Dictionary<int, object> _documentDataContainerLocks;
        private bool _disposed;

        public DataContainer(ITracer tracer, DataContainerDescriptor dataContainerDescriptor, string storageRoot)
        {

            // intentionally allowed to be null - in case if we don't want to use any persistence
            _storageRoot = storageRoot;
            _tracer = tracer ?? throw new ArgumentNullException(nameof(tracer));
            _memoryPool = new DynamicMemoryPool();

            _dataContainerDescriptor = dataContainerDescriptor ?? throw new ArgumentNullException(nameof(dataContainerDescriptor));
            _documentDataContainers = new Dictionary<int, DocumentDataContainer>(50);
            _documentDataContainerLocks = new Dictionary<int, object>(50);

            foreach (var item in dataContainerDescriptor.EnumerateDocumentTypes())
            {
                _documentDataContainerLocks.Add(item.DocumentType, new object());
            }
        }

        public DocumentDataContainer RequireDocumentContainer(int docType)
        {
            if (_documentDataContainers.TryGetValue(docType, out var docStore))
            {
                return docStore;
            }

            var doclock = _documentDataContainerLocks[docType];
            lock (doclock)
            {
                if (!_documentDataContainers.TryGetValue(docType, out docStore))
                {
                    docStore = new DocumentDataContainer(
                        _dataContainerDescriptor,
                        _dataContainerDescriptor.RequireDocumentType(docType),
                        _memoryPool,
                        _tracer);
                    if (!string.IsNullOrEmpty(_storageRoot))
                    {
                        var stats = ReadStatsFromStore(_storageRoot);
                        docStore.ReadDataFromStore(GetDocRootPath(_storageRoot, _dataContainerDescriptor.RequireDocumentType(docType)), stats.TryGetDocumentCount(docType));
                    }

                    _documentDataContainers.Add(docType, docStore);
                }

                return docStore;
            }
        }

        public void FlushDataToStore()
        {
            if (string.IsNullOrEmpty(_storageRoot))
            {
                return;
            }

            if (!Directory.Exists(_storageRoot))
            {
                throw new ArgumentException("Storage root is invalid: " + _storageRoot);
            }

            WriteStatsToStore();

            foreach (var pair in _documentDataContainers)
            {
                var docTypeDesc = _dataContainerDescriptor.RequireDocumentType(pair.Key);
                var docRootPath = Path.Combine(_storageRoot, GetFolderName(docTypeDesc));
                Directory.CreateDirectory(docRootPath);

                RequireDocumentContainer(pair.Key).FlushDataToStore(docRootPath);
            }
        }

        public void ReadDataFromStore()
        {
            if (string.IsNullOrEmpty(_storageRoot))
            {
                return;
            }

            if (!Directory.Exists(_storageRoot))
            {
                throw new ArgumentException("Storage root is invalid: " + _storageRoot);
            }

            // read stats to verify that file exists
            ReadStatsFromStore(_storageRoot);
        }

        public void WriteStatsToStore()
        {
            var stats = new DataContainerStats();
            foreach (var pair in _documentDataContainers)
            {
                var docStore = RequireDocumentContainer(pair.Key);
                stats.SetDocumentCount(pair.Key, docStore.UntrimmedCount);
            }

            WriteStatsToStore(stats, _storageRoot);
        }

        public void WriteDescriptorToStore()
        {
            WriteDescriptorToStore(_dataContainerDescriptor, _storageRoot);
        }

        public static void WriteStatsToStore(DataContainerStats stats, string storageRoot)
        {
            if (string.IsNullOrEmpty(storageRoot))
            {
                throw new ArgumentNullException(storageRoot);
            }

            if (stats == null)
            {
                throw new ArgumentNullException(nameof(stats));
            }

            Directory.CreateDirectory(storageRoot);

            var file = new DataContainerStatsFile(RamDriverFactory.CurrentStoreVersion().ToString(), stats);
            using var writer = new FileStream(Path.Combine(storageRoot, "stats.json"), FileMode.Create, FileAccess.ReadWrite);
            JsonSerializer.Serialize(writer, file);
            writer.Flush();
        }

        public static void WriteDescriptorToStore(DataContainerDescriptor descriptor, string storageRoot)
        {
            if (string.IsNullOrEmpty(storageRoot))
            {
                throw new ArgumentNullException(storageRoot);
            }

            if (descriptor == null)
            {
                throw new ArgumentNullException(nameof(descriptor));
            }

            Directory.CreateDirectory(storageRoot);

            var file = new DataContainerDescriptorFile(RamDriverFactory.CurrentStoreVersion(), descriptor);
            using var writer = new FileStream(Path.Combine(storageRoot, "descriptor.json"), FileMode.Create, FileAccess.ReadWrite);
            JsonSerializer.Serialize(writer, file);
            writer.Flush();
        }

        private string GetFolderName(DocumentTypeDescriptor docTypeDesc)
        {
            return string.Format("{0}-{1}", docTypeDesc.BaseDatasetName ?? docTypeDesc.Name, docTypeDesc.DocumentType);
        }

        private string GetDocRootPath(string storageRoot, DocumentTypeDescriptor docTypeDesc)
        {
            return Path.Combine(storageRoot, GetFolderName(docTypeDesc));
        }

        private static DataContainerStats ReadStatsFromStore(string storageRoot)
        {
            var path = Path.Combine(storageRoot, "stats.json");
            if (!File.Exists(path))
            {
                return new DataContainerStats();
            }

            using var reader = new StreamReader(new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read));
            var serializer = JsonSerializer.Create(
                new JsonSerializerSettings
                {
                    ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor
                });

            var file = (DataContainerStatsFile)serializer.Deserialize(reader, typeof(DataContainerStatsFile));
            var storedVersion = new Version(file.DriverVersion);
            var minCompatible = RamDriverFactory.MinCompatibleStoreVersion();
            if (minCompatible.CompareTo(storedVersion) > 0)
            {
                throw new Exception(
                    string.Format(
                        "Version of storage stats is too old. Found: {0}, minimum supported: {1}",
                        file.DriverVersion, minCompatible));
            }

            if (file.DataContainerStats == null)
            {
                throw new Exception("Stats file is empty");
            }

            return file.DataContainerStats;
        }

        public void Dispose()
        {
            Dispose(true);
        }

        private void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                _disposed = true;

                if (disposing)
                {
                    GC.SuppressFinalize(this);
                }

                foreach (var c in _documentDataContainers)
                {
                    c.Value.Dispose();
                }

                _memoryPool.Dispose();
            }
        }

        ~DataContainer()
        {
            Dispose(false);
        }

        public void Compact(CompactionOptions options)
        {
            if (options == CompactionOptions.PurgeDeleted)
            {
                _memoryPool.DeallocateGarbage();
            }
            else if (options == CompactionOptions.FullReindex)
            {
                RebuildUnmanagedData();

                foreach (var c in _documentDataContainers)
                {
                    c.Value.SortIndexManager.InvalidateAllIndexes();
                }
            }
            else
            {
                throw new ArgumentException("Compaction mode not supported yet: " + options);
            }
        }

        private void RebuildUnmanagedData()
        {
            var newpool = new DynamicMemoryPool();

            Action<object> action = x => ((DocumentDataContainer)x).MigrateRAM(newpool);
            var tasks = _documentDataContainers.Values.Select(x =>
                {
                    var x1 = x;
                    return new Task(action, x1);
                }).ToArray();

            try
            {
                try
                {
                    foreach (var t in tasks)
                    {
                        t.Start();
                    }

                    Task.WaitAll(tasks);
                }
                catch
                {
                    newpool.Dispose();
                    throw;
                }

                try
                {
                    _memoryPool.Dispose();
                }
                finally
                {
                    _memoryPool = newpool;
                }
            }
            catch
            {
                foreach (var c in _documentDataContainers.Values)
                {
                    c.MarkAsInvalid();
                }

                throw;
            }
        }
    }
}