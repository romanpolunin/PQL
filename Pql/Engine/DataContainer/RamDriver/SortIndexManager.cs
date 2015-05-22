using System;
using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace Pql.Engine.DataContainer.RamDriver
{
    internal class SortIndexManager
    {
        private readonly ConcurrentDictionary<int, int> m_fieldIdToIndexHandle;
        private readonly SortIndex[] m_fieldIndexes;
        private readonly DocumentDataContainer m_documentStore;

        public SortIndexManager(DocumentDataContainer documentStore)
        {
            if (documentStore == null)
            {
                throw new ArgumentNullException("documentStore");
            }

            m_documentStore = documentStore;
            m_fieldIdToIndexHandle = new ConcurrentDictionary<int, int>();
            
            if (m_documentStore.FieldIdToColumnStore.Count != m_documentStore.DocDesc.Fields.Length)
            {
                throw new Exception("Internal error: fieldid->colstore map and array of fields in document descriptor have different lengths");
            }
            
            m_fieldIndexes = new SortIndex[m_documentStore.FieldIdToColumnStore.Count];
            for (var ordinal = 0; ordinal < m_fieldIndexes.Length; ordinal++)
            {
                m_fieldIndexes[ordinal] = new SortIndex();
                m_fieldIdToIndexHandle[m_documentStore.DocDesc.Fields[ordinal]] = ordinal;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InvalidateIndex(int fieldId)
        {
            int handle;
            if (m_fieldIdToIndexHandle.TryGetValue(fieldId, out handle))
            {
                m_fieldIndexes[handle].Invalidate();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InvalidateAllIndexes()
        {
            foreach (var sortIndex in m_fieldIndexes) 
            {
                sortIndex.Invalidate();
            }
        }

        public SortIndex GetIndex(int fieldId, int count)
        {
            var index = m_fieldIndexes[m_fieldIdToIndexHandle[fieldId]];

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
                var columnStore = m_documentStore.RequireColumnStore(fieldId);
                
                var method = typeof (SortIndex).GetMethod("Update").MakeGenericMethod(columnStore.ElementType);
                
                m_documentStore.StructureLock.EnterWriteLock();
                try
                {
                    method.Invoke(index, new object[] { columnStore, m_documentStore.ValidDocumentsBitmap, count });
                }
                catch (TargetInvocationException e)
                {
                    throw e.InnerException;
                }
                finally
                {
                    m_documentStore.StructureLock.ExitWriteLock();
                }
            }
        }
    }
}