using System.Collections.Concurrent;
using System.Data;

using Pql.SqlEngine.Interfaces;
using Pql.SqlEngine.Interfaces.Internal;
using Pql.SqlEngine.Interfaces.Services;

namespace Pql.SqlEngine.DataContainer.RamDriver
{
    /// <summary>
    /// Constructs instances of <see cref="RamDriver"/>.
    /// </summary>
    public class RamDriverFactory : IStorageDriverFactory
    {
        /// <summary>
        /// Creates a new uninitialized instance of a storage driver.
        /// </summary>
        /// <seealso cref="IStorageDriverFactory.GetDriverConfig"/>
        public IStorageDriver Create()
        {
            return new RamDriver();
        }

        /// <summary>
        /// Reads configuration settings object for a particular scope.
        /// Settings object type is specific to implementation.
        /// </summary>
        /// <param name="scopeId">ScopeId is used to locate data store</param>
        public object GetDriverConfig(string scopeId)
        {
            return null;
        }

        Version IStorageDriverFactory.CurrentStoreVersion()
        {
            return CurrentStoreVersion();
        }

        Version IStorageDriverFactory.MinCompatibleStoreVersion()
        {
            return MinCompatibleStoreVersion();
        }

        /// <summary>
        /// Returns current version information for storage format.
        /// </summary>
        public static Version CurrentStoreVersion()
        {
            return new Version(0, 0, 0, 0);
        }

        /// <summary>
        /// Returns minimum compatible version number of storage format.
        /// </summary>
        public static Version MinCompatibleStoreVersion()
        {
            return new Version(0, 0, 0, 0);
        }
    }

    /// <summary>
    /// Settings object for <see cref="RamDriver"/>.
    /// </summary>
    public class RamDriverSettings
    {
        /// <summary>
        /// Use this to operate <see cref="RamDriver"/> in different modes or initialize in a specific way.
        /// Currently supported commands: "demo".
        /// </summary>
        public string InitializationCommand;
        /// <summary>
        /// File directory that holds data files.
        /// </summary>
        public string StorageRoot;
        /// <summary>
        /// Container descriptor.
        /// </summary>
        public DataContainerDescriptor Descriptor;

        /// <summary>
        /// Ctr.
        /// </summary>
        public RamDriverSettings()
        {}

        /// <summary>
        /// Ctr.
        /// </summary>
        public RamDriverSettings(RamDriverSettings settings)
        {
            if (settings != null)
            {
                InitializationCommand = settings.InitializationCommand;
                StorageRoot = settings.StorageRoot;
                Descriptor = settings.Descriptor;
            }
        }
    }
    
    /// <summary>
    /// Implements <see cref="IStorageDriver"/> by holding all data in RAM.
    /// Supports serialization to disk.
    /// </summary>
    public class RamDriver : IStorageDriver
    {
        private ITracer _tracer;
        private volatile bool _initialized;
        private volatile DataContainerDescriptor _descriptor;
        private DataContainer _dataContainer;
        private readonly object _thisLock;
        private ConcurrentDictionary<long, RamDriverChangeset> _changesets;
        private long _lastChangesetHandle;
        private RamDriverSettings _settings;
        
        /// <summary>
        /// Ctr.
        /// </summary>
        public RamDriver()
        {
            _thisLock = new object();
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
            _initialized = false;

            if (_dataContainer != null)
            {
                _dataContainer.Dispose();
                _dataContainer = null;
            }
        }

        /// <summary>
        /// Initializes driver with settings object.
        /// </summary>
        /// <param name="tracer">Tracer sink</param>
        /// <param name="settings">Arbitrary settings object, type specific to implementation</param>
        /// <seealso cref="IStorageDriverFactory.Create"/>
        /// <seealso cref="IStorageDriverFactory.GetDriverConfig"/>
        public void Initialize(ITracer tracer, object settings)
        {
            if (_initialized)
            {
                throw new InvalidOperationException("Already initialized");
            }

            _tracer = tracer ?? throw new ArgumentNullException(nameof(tracer));
            if (_tracer.IsDebugEnabled)
            {
                _tracer.Debug(string.Concat("Initializing RAM storage driver for settings string: ", settings));
            }

            _changesets = new ConcurrentDictionary<long, RamDriverChangeset>();
            
            var initString = settings as string;
            if (!string.IsNullOrEmpty(initString))
            {
                _settings = new RamDriverSettings();
                if (0 == StringComparer.OrdinalIgnoreCase.Compare("demo", initString))
                {
                    _settings.InitializationCommand = initString;
                }
                else
                {
                    _settings.StorageRoot = initString;
                }
            }
            else
            {
                _settings = new RamDriverSettings(settings as RamDriverSettings);
            }

            CheckInitialized();

            if (_tracer.IsInfoEnabled)
            {
                _tracer.Info("RAM storage driver ready");
            }
        }

        /// <summary>
        /// Reads data container descriptor from underlying store.
        /// </summary>
        public DataContainerDescriptor GetDescriptor()
        {
            CheckInitialized();
            return _descriptor;
        }

        private static DataContainerDescriptor BuildDemoContainerDescriptor()
        {
            var result = new DataContainerDescriptor();
            result.AddDocumentTypeName("testDoc");

            var testDocId = result.RequireDocumentTypeName("testDoc");
            var count = 12;

            for (var fieldId = 1; fieldId <= count; )
            {
                result.AddField(new FieldMetadata(fieldId, "Field" + fieldId, "Byte" + fieldId, DbType.Byte, testDocId));
                fieldId++;
                result.AddField(new FieldMetadata(fieldId, "Field" + fieldId, "Guid" + fieldId, DbType.Guid, testDocId));
                fieldId++;
                result.AddField(new FieldMetadata(fieldId, "Field" + fieldId, "String" + fieldId, DbType.String, testDocId));
                fieldId++;
                result.AddField(new FieldMetadata(fieldId, "Field" + fieldId, "Binary" + fieldId, DbType.Binary, testDocId));
                fieldId++;
                result.AddField(new FieldMetadata(fieldId, "Field" + fieldId, "Decimal" + fieldId, DbType.Decimal, testDocId));
                fieldId++;
                result.AddField(new FieldMetadata(fieldId, "Field" + fieldId, "Bool" + fieldId, DbType.Boolean, testDocId));
                fieldId++;
            }

            result.AddField(new FieldMetadata(count + 1, "id", "primary key", DbType.Int64, testDocId));

            var fieldIds = result.EnumerateFields().Select(x => x.FieldId).ToArray();
            result.AddDocumentTypeDescriptor(new DocumentTypeDescriptor("testDoc", "testDoc", 1, "id", fieldIds));

            return result;
        }

        public DataContainerStats GetStats()
        {
            CheckHaveDescriptor();
            var stats = new DataContainerStats();
            foreach (var doc in _descriptor.EnumerateDocumentTypes())
            {
                stats.SetDocumentCount(doc.DocumentType, _dataContainer.RequireDocumentContainer(doc.DocumentType).UntrimmedCount);
            }

            return stats;
        }

        public void BeginPrepareColumnData(int fieldId)
        {
            var docType = _descriptor.RequireField(fieldId).OwnerDocumentType;
            _dataContainer.RequireDocumentContainer(docType).BeginLoadColumnStore(fieldId);
        }

        public void PrepareAllColumnsAndWait(int docType)
        {
            var docDesc = GetDescriptor().RequireDocumentType(docType);
            var docStore = _dataContainer.RequireDocumentContainer(docType);
            
            foreach (var fieldId in docDesc.Fields)
            {
                docStore.BeginLoadColumnStore(fieldId);
            }
            
            foreach (var fieldId in docDesc.Fields)
            {
                docStore.RequireColumnStore(fieldId).WaitLoadingCompleted();
            }
        }

        /// <summary>
        /// Schedules asynchronous loading of all columns and then waits for them to complete.
        /// </summary>
        public void PrepareAllEntitiesAndWait()
        {
            // schedule loading of all columns
            foreach (var docDesc in GetDescriptor().EnumerateDocumentTypes())
            {
                var docStore = _dataContainer.RequireDocumentContainer(docDesc.DocumentType);

                foreach (var fieldId in docDesc.Fields)
                {
                    docStore.BeginLoadColumnStore(fieldId);
                }
            }

            // wait until all columns are loaded
            foreach (var docDesc in GetDescriptor().EnumerateDocumentTypes())
            {
                var docStore = _dataContainer.RequireDocumentContainer(docDesc.DocumentType);

                foreach (var fieldId in docDesc.Fields)
                {
                    docStore.RequireColumnStore(fieldId).WaitLoadingCompleted();
                }
            }
        }

        /// <summary>
        /// Enumerates through data for a particular SELECT query.
        /// Inside MoveNext() imlementation, MUST populate the same instance of row data, pointed to by <see cref="RequestExecutionContext.DriverOutputBuffer"/>.
        /// Yields dummy true value, its value and data type are reserved for future use.
        /// </summary>
        public IDriverDataEnumerator GetData(RequestExecutionContext context)
        {
            CheckInitialized();

            var data = _dataContainer.RequireDocumentContainer(context.ParsedRequest.TargetEntity.DocumentType);

            if (context.ParsedRequest.IsBulk)
            {
                // engine expects us to use the bulk input data iterator to fetch data rows
                return data.GetBulkUpdateEnumerator(
                    context.ParsedRequest.BaseDataset.BaseFields,
                    context.DriverOutputBuffer,
                    context.InputDataEnumerator);
            }

            if (context.ParsedRequest.BaseDataset.OrderClauseFields.Count == 0)
            {
                return data.GetUnorderedEnumerator(
                    context.ParsedRequest.BaseDataset.BaseFields, 
                    context.ParsedRequest.BaseDataset.BaseFieldsMainCount, 
                    context.DriverOutputBuffer);
            }

            if (context.ParsedRequest.BaseDataset.OrderClauseFields.Count > 1)
            {
                throw new InvalidOperationException("Sorting by more than one field is not supported yet");
            }

            return data.GetOrderedEnumerator(
                context.ParsedRequest.BaseDataset.BaseFields,
                context.ParsedRequest.BaseDataset.BaseFieldsMainCount,
                context.DriverOutputBuffer,
                context.ParsedRequest.BaseDataset.OrderClauseFields[0].Item1,
                context.ParsedRequest.BaseDataset.OrderClauseFields[0].Item2);
        }

        public long CreateChangeset(DriverChangeBuffer changeBuffer, bool isBulk)
        {
            CheckInitialized();

            if (changeBuffer == null)
            {
                throw new ArgumentNullException(nameof(changeBuffer));
            }

            var key = Interlocked.Increment(ref _lastChangesetHandle);

            ColumnDataBase[] columnStores = null;
            var documentContainer = _dataContainer.RequireDocumentContainer(changeBuffer.TargetEntity);
            if (changeBuffer.Fields != null && changeBuffer.Fields.Length > 0)
            {
                columnStores = new ColumnDataBase[changeBuffer.Fields.Length];
                for (var i = 0; i < changeBuffer.Fields.Length; i++)
                {
                    var field = changeBuffer.Fields[i];
                    columnStores[i] = documentContainer.RequireColumnStore(field.FieldId);
                }
            }

            var changesetRec = new RamDriverChangeset(this, changeBuffer, isBulk, documentContainer, columnStores);
            
            documentContainer.StructureLock.EnterReadLock();
            try
            {
                if (!_changesets.TryAdd(key, changesetRec))
                {
                    throw new Exception("Internal error");
                }
                return key;
            }
            catch
            {
                documentContainer.StructureLock.ExitReadLock();
                throw;
            }
        }

        public void AddChange(long changeset)
        {
            CheckInitialized();

            var changesetRec = _changesets[changeset];
            bool success;

            switch (changesetRec.ChangeBuffer.ChangeType)
            {
                case DriverChangeType.Insert:
                    InsertOne(changesetRec);
                    success = true;
                    break;
                case DriverChangeType.Update:
                    success = UpdateOne(changesetRec);
                    break;
                case DriverChangeType.Delete:
                    success = DeleteOne(changesetRec);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(changeset), changesetRec.ChangeBuffer.ChangeType, "Change type has invalid value");
            }

            if (success)
            {
                // TODO: interlocked?
				changesetRec.ChangeCount++;
            }
        }

        private static void InvalidateIndexes(RamDriverChangeset changesetRec)
        {
            if (changesetRec.ChangeBuffer.ChangeType == DriverChangeType.Update 
                || changesetRec.ChangeBuffer.ChangeType == DriverChangeType.Insert)
            {
                foreach (var field in changesetRec.ChangeBuffer.Fields)
                {
                    changesetRec.DocumentContainer.SortIndexManager.InvalidateIndex(field.FieldId);
                }
            }
            else
            {
                changesetRec.DocumentContainer.SortIndexManager.InvalidateAllIndexes();
            }
        }

        private bool DeleteOne(RamDriverChangeset changesetRec)
        {
            return changesetRec.DocumentContainer.TryDeleteDocument(changesetRec.ChangeBuffer.InternalEntityId);
        }

        private bool UpdateOne(RamDriverChangeset changesetRec)
        {
            int docIndex = 0;
            var entityId = changesetRec.ChangeBuffer.InternalEntityId;
            if (changesetRec.DocumentContainer.DocumentIdToIndex.TryGetValueInt32(entityId, ref docIndex))
            {
                UpdateAtPosition(changesetRec, changesetRec.ChangeBuffer, docIndex);
                return true;
            }
            return false;
        }

        private void InsertOne(RamDriverChangeset changesetRec)
        {
            changesetRec.DocumentContainer.TryAddDocument(changesetRec.ChangeBuffer.InternalEntityId, out var docIndex);
            UpdateAtPosition(changesetRec, changesetRec.ChangeBuffer, docIndex);
        }

        public void AllocateCapacityForDocumentType(int documentType, int additionalCapacity)
        {
            CheckInitialized();

            var documentContainer = _dataContainer.RequireDocumentContainer(documentType);
            documentContainer.StructureLock.EnterReadLock();
            try
            {
                documentContainer.AllocateAdditionalCapacity(additionalCapacity);
            }
            finally
            {
                documentContainer.StructureLock.ExitReadLock();
            }
        }

        private void UpdateAtPosition(RamDriverChangeset changesetRec, DriverChangeBuffer change, int docIndex)
        {
            var changeData = change.Data;
            for (var ordinal = 0; ordinal < change.Fields.Length; ordinal++)
            {
                var colStore = changesetRec.ColumnStores[ordinal];

                if (BitVector.Get(changeData.NotNulls, ordinal))
                {
                    colStore.NotNulls.SafeSet(docIndex);
                    var indexInArray = changeData.GetIndexInArray(ordinal);
                    colStore.AssignFromDriverRow(docIndex, changeData, indexInArray);
                }
                else
                {
                    colStore.NotNulls.SafeClear(docIndex);
                }
            }
        }

        public int Apply(long changeset)
        {
            CheckInitialized();

            if (!_changesets.TryRemove(changeset, out var changesetRec))
            {
                throw new ArgumentException("Invalid changeset handle: " + changeset, nameof(changeset));
            }

            try
            {
                InvalidateIndexes(changesetRec);
            }
            finally
            {
                changesetRec.DocumentContainer.StructureLock.ExitReadLock();
            }

            if (changesetRec.ChangeCount > 0)
            {
                if (_tracer.IsDebugEnabled)
                {
                    _tracer.Debug(
                        string.Format(
                            "Applied {0} changed rows on changeset {1}", changesetRec.ChangeCount, changeset));
                }
            }
            else
            {
                if (_tracer.IsDebugEnabled)
                {
                    _tracer.Debug("Did not have any changes to apply on changeset " + changeset);
                }
            }

            return changesetRec.ChangeCount;
        }

        public void Discard(long changeset)
        {
            CheckInitialized();


            if (_changesets.TryRemove(changeset, out var changesetRec))
            {
                try
                {
                    InvalidateIndexes(changesetRec);
                }
                finally
                {
                    changesetRec.DocumentContainer.StructureLock.ExitReadLock();
                }
            }
        }

        public void Compact(CompactionOptions options)
        {
            CheckInitialized();
            _dataContainer.Compact(options);
        }

        public void FlushDataToStore()
        {
            CheckInitialized();

            PrepareAllEntitiesAndWait();
            _dataContainer.WriteDescriptorToStore();
            _dataContainer.WriteStatsToStore();
            _dataContainer.FlushDataToStore();
        }

        public bool CanUpdateField(int fieldId)
        {
            CheckInitialized();

            var docType = _descriptor.RequireField(fieldId).OwnerDocumentType;
            return fieldId != _dataContainer.RequireDocumentContainer(docType).PrimaryKeyFieldId;
        }

        private DataContainerDescriptor GetDescriptorFromStore()
        {
            if (string.IsNullOrEmpty(_settings.StorageRoot))
            {
                throw new Exception("Storage root is not set");
            }

            var path = Path.Combine(_settings.StorageRoot, "descriptor.json");
            if (!File.Exists(path))
            {
                throw new Exception("Descriptor file does not exist in storage root: " + _settings.StorageRoot);
            }

            try
            {
                using var reader = new StreamReader(new FileStream(path, FileMode.Open, FileAccess.Read));
                var serializer = JsonSerializer.Create(new JsonSerializerSettings
                {
                    ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor
                });

                var file =
                    (DataContainerDescriptorFile)
                    serializer.Deserialize(reader, typeof(DataContainerDescriptorFile));
                var storedVersion = new Version(file.DriverVersion);
                var minCompatible = RamDriverFactory.MinCompatibleStoreVersion();
                if (minCompatible.CompareTo(storedVersion) > 0)
                {
                    throw new Exception(string.Format(
                        "Version of storage descriptor is too old. Found: {0}, minimum supported: {1}",
                        file.DriverVersion, minCompatible));
                }

                if (file.DataContainerDescriptor == null)
                {
                    throw new Exception("Descriptor file is empty");
                }

                return file.DataContainerDescriptor;
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Could not read descriptor from store: " + _settings.StorageRoot, e);
            }
        }

        private void CheckInitialized()
        {
            if (_initialized)
            {
                return;
            }

            lock (_thisLock)
            {
                if (_initialized)
                {
                    return;
                }

                if (0 == StringComparer.OrdinalIgnoreCase.Compare("demo", _settings.InitializationCommand))
                {
                    _descriptor = BuildDemoContainerDescriptor();
                }
                else if (_settings.Descriptor != null)
                {
                    _descriptor = _settings.Descriptor;
                }
                else
                {
                    _descriptor = GetDescriptorFromStore();
                }

                if (_descriptor != null)
                {
                    _dataContainer = new DataContainer(_tracer, _descriptor, _settings.StorageRoot);
                    _initialized = true;
                }
            }
        }

        private void CheckHaveDescriptor()
        {
            if (_descriptor == null)
            {
                throw new InvalidOperationException("Container descriptor is not set");
            }
        }
    }

    internal class RamDriverChangeset
    {
        public readonly RamDriver Driver;
        public readonly DriverChangeBuffer ChangeBuffer;
        public readonly bool IsBulk;
        public readonly ColumnDataBase[] ColumnStores;
        public readonly DocumentDataContainer DocumentContainer;
        public int ChangeCount;

        /// <summary>
        /// Ctr.
        /// </summary>
        public RamDriverChangeset(RamDriver driver, DriverChangeBuffer changeBuffer, bool isBulk, DocumentDataContainer documentContainer, ColumnDataBase[] columnStores)
        {
            Driver = driver ?? throw new ArgumentNullException(nameof(driver));
            ChangeBuffer = changeBuffer ?? throw new ArgumentNullException(nameof(changeBuffer));
            IsBulk = isBulk;
            ColumnStores = columnStores;
            DocumentContainer = documentContainer ?? throw new ArgumentNullException(nameof(documentContainer));
        }
    }
}
