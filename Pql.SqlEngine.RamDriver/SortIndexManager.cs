using System.Collections.Concurrent;
using System.Reflection;

namespace Pql.SqlEngine.DataContainer.RamDriver
{
    internal class SortIndexManager
    {
        private readonly ConcurrentDictionary<int, int> _fieldIdToIndexHandle;
        private readonly SortIndex[] _fieldIndexes;
        private readonly DocumentDataContainer _documentStore;

        public SortIndexManager(DocumentDataContainer documentStore)
        {
            _documentStore = documentStore ?? throw new ArgumentNullException(nameof(documentStore));
            _fieldIdToIndexHandle = new ConcurrentDictionary<int, int>();
            
            if (_documentStore.FieldIdToColumnStore.Count != _documentStore.DocDesc.Fields.Length)
            {
                throw new Exception("Internal error: fieldid->colstore map and array of fields in document descriptor have different lengths");
            }
            
            _fieldIndexes = new SortIndex[_documentStore.FieldIdToColumnStore.Count];
            for (var ordinal = 0; ordinal < _fieldIndexes.Length; ordinal++)
            {
                _fieldIndexes[ordinal] = new SortIndex();
                _fieldIdToIndexHandle[_documentStore.DocDesc.Fields[ordinal]] = ordinal;
            }
        }

        
        public void InvalidateIndex(int fieldId)
        {
            if (_fieldIdToIndexHandle.TryGetValue(fieldId, out var handle))
            {
                _fieldIndexes[handle].Invalidate();
            }
        }

        
        public void InvalidateAllIndexes()
        {
            foreach (var sortIndex in _fieldIndexes) 
            {
                sortIndex.Invalidate();
            }
        }

        public SortIndex GetIndex(int fieldId, int count)
        {
            var index = _fieldIndexes[_fieldIdToIndexHandle[fieldId]];

            if (!index.IsValid)
            {
                lock (index)
                {
                    UpdateIndex(fieldId, index, count);
                }
            }

            return index;
        }

        internal void UpdateIndex(int fieldId, SortIndex index, int count)
        {
            if (!index.IsValid)
            {
                var columnStore = _documentStore.RequireColumnStore(fieldId);
                
                var method = typeof (SortIndex).GetMethod("Update").MakeGenericMethod(columnStore.ElementType);
                
                _documentStore.StructureLock.EnterWriteLock();
                try
                {
                    method.Invoke(index, new object[] { columnStore, _documentStore.ValidDocumentsBitmap, count });
                }
                catch (TargetInvocationException e)
                {
                    throw e.InnerException;
                }
                finally
                {
                    _documentStore.StructureLock.ExitWriteLock();
                }
            }
        }
    }
}