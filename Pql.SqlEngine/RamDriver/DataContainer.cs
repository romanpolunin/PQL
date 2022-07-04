using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Pql.Engine.Interfaces;
using Pql.Engine.Interfaces.Internal;
using Pql.Engine.Interfaces.Services;
using Pql.UnmanagedLib;

namespace Pql.Engine.DataContainer.RamDriver
{
    internal sealed class DataContainer : IDisposable
    {
        private readonly ITracer m_tracer;
        private volatile IUnmanagedAllocator m_memoryPool;
        private readonly DataContainerDescriptor m_dataContainerDescriptor;
        private readonly string m_storageRoot;
        private readonly Dictionary<int, DocumentDataContainer> m_documentDataContainers;
        private readonly Dictionary<int, object> m_documentDataContainerLocks;
        private bool m_disposed;

        public DataContainer(ITracer tracer, DataContainerDescriptor dataContainerDescriptor, string storageRoot)
        {

            // intentionally allowed to be null - in case if we don't want to use any persistence
            m_storageRoot = storageRoot;
            m_tracer = tracer ?? throw new ArgumentNullException(nameof(tracer));
            m_memoryPool = new DynamicMemoryPool();

            m_dataContainerDescriptor = dataContainerDescriptor ?? throw new ArgumentNullException(nameof(dataContainerDescriptor));
            m_documentDataContainers = new Dictionary<int, DocumentDataContainer>(50);
            m_documentDataContainerLocks = new Dictionary<int, object>(50);

            foreach (var item in dataContainerDescriptor.EnumerateDocumentTypes())
            {
                m_documentDataContainerLocks.Add(item.DocumentType, new object());
            }
        }

        public DocumentDataContainer RequireDocumentContainer(int docType)
        {
            if (m_documentDataContainers.TryGetValue(docType, out var docStore))
            {
                return docStore;
            }

            var doclock = m_documentDataContainerLocks[docType];
            lock (doclock)
            {
                if (!m_documentDataContainers.TryGetValue(docType, out docStore))
                {
                    docStore = new DocumentDataContainer(
                        m_dataContainerDescriptor, 
                        m_dataContainerDescriptor.RequireDocumentType(docType),
                        m_memoryPool,
                        m_tracer);
                    if (!string.IsNullOrEmpty(m_storageRoot))
                    {
                        var stats = ReadStatsFromStore(m_storageRoot);
                        docStore.ReadDataFromStore(GetDocRootPath(m_storageRoot, m_dataContainerDescriptor.RequireDocumentType(docType)), stats.TryGetDocumentCount(docType));
                    }
                    
                    m_documentDataContainers.Add(docType, docStore);
                }

                return docStore;
            }
        }

        public void FlushDataToStore()
        {
            if (string.IsNullOrEmpty(m_storageRoot))
            {
                return;
            }
            
            if (!Directory.Exists(m_storageRoot))
            {
                throw new ArgumentException("Storage root is invalid: " + m_storageRoot);
            }

            WriteStatsToStore();

            foreach (var pair in m_documentDataContainers)
            {
                var docTypeDesc = m_dataContainerDescriptor.RequireDocumentType(pair.Key);
                var docRootPath = Path.Combine(m_storageRoot, GetFolderName(docTypeDesc));
                Directory.CreateDirectory(docRootPath);

                RequireDocumentContainer(pair.Key).FlushDataToStore(docRootPath);
            }
        }

        public void ReadDataFromStore()
        {
            if (string.IsNullOrEmpty(m_storageRoot))
            {
                return;
            }

            if (!Directory.Exists(m_storageRoot))
            {
                throw new ArgumentException("Storage root is invalid: " + m_storageRoot);
            }

            // read stats to verify that file exists
            ReadStatsFromStore(m_storageRoot);
        }

        public void WriteStatsToStore()
        {
            var stats = new DataContainerStats();
            foreach (var pair in m_documentDataContainers)
            {
                var docStore = RequireDocumentContainer(pair.Key);
                stats.SetDocumentCount(pair.Key, docStore.UntrimmedCount);
            }

            WriteStatsToStore(stats, m_storageRoot);
        }

        public void WriteDescriptorToStore()
        {
            WriteDescriptorToStore(m_dataContainerDescriptor, m_storageRoot);
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
            
            var file = new DataContainerStatsFile(RamDriverFactory.CurrentStoreVersion(), stats);
            using var writer = new StreamWriter(
                new FileStream(Path.Combine(storageRoot, "stats.json"), FileMode.Create, FileAccess.ReadWrite));
            var serializer = new JsonSerializer();
            serializer.Serialize(writer, file);
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
            using var writer = new StreamWriter(
                new FileStream(Path.Combine(storageRoot, "descriptor.json"), FileMode.Create, FileAccess.ReadWrite));
            var serializer = new JsonSerializer();
            serializer.Serialize(writer, file);
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
            if (!m_disposed)
            {
                m_disposed = true;

                if (disposing)
                {
                    GC.SuppressFinalize(this);
                }

                foreach (var c in m_documentDataContainers)
                {
                    c.Value.Dispose();
                }

                m_memoryPool.Dispose();
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
                m_memoryPool.DeallocateGarbage();
            }
            else if (options == CompactionOptions.FullReindex)
            {
                RebuildUnmanagedData();

                foreach (var c in m_documentDataContainers)
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
            var tasks = m_documentDataContainers.Values.Select(x =>
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
                    m_memoryPool.Dispose();
                }
                finally
                {
                    m_memoryPool = newpool;
                }
            }
            catch
            {
                foreach (var c in m_documentDataContainers.Values)
                {
                    c.MarkAsInvalid();
                }

                throw;
            }
        }
    }
}