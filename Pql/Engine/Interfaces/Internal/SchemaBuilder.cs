using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Pql.Engine.Interfaces.Internal
{
    public class SchemaBuilder
    {
        private DataContainerDescriptor m_descriptor;
        private Dictionary<int, Dictionary<DbType, int>> m_fieldsMap;
        private int m_lastFieldId;

        public SchemaBuilder()
        {
            m_descriptor = new DataContainerDescriptor();
        }

        public void AddDocumentTypeNames(params string[] docTypeNames)
        {
            if (docTypeNames == null)
            {
                throw new ArgumentNullException("docTypeNames");
            }

            if (m_fieldsMap != null)
            {
                throw new InvalidOperationException("Cannot add document type names after BeginDefineDocumentTypes was called");
            }

            if (m_descriptor == null)
            {
                throw new InvalidOperationException("Cannot add document type names after Commit was called");
            }

            foreach (var name in docTypeNames)
            {
                m_descriptor.AddDocumentTypeName(name);
            }
        }

        public void BeginDefineDocumentTypes()
        {
            if (m_fieldsMap != null)
            {
                throw new InvalidOperationException("Cannot invoke BeginDefineDocumentTypes more than once");
            }

            if (m_descriptor == null)
            {
                throw new InvalidOperationException("Cannot define document types after Commit was called");
            } 
            
            m_fieldsMap = InitializeFieldsMap(m_descriptor.EnumerateDocumentTypeNames());
            
            m_lastFieldId = 0;
        }

        public DocumentTypeDescriptor AddDocumentTypeDescriptor(string docTypeName, string baseDatasetName, params object[] data)
        {
            return AddDocumentTypeDescriptorWithPrimaryKey(docTypeName, baseDatasetName, null, data);
        }

        public DocumentTypeDescriptor AddDocumentTypeDescriptorWithPrimaryKey(string docTypeName, string baseDatasetName, string primaryKeyFieldName, params object[] data)
        {
            if (string.IsNullOrEmpty(docTypeName))
            {
                throw new ArgumentNullException(docTypeName);
            }

            if (data == null)
            {
                throw new ArgumentNullException("data");
            }

            if (data.Length == 0 || 0 != data.Length % 2)
            {
                throw new ArgumentException("Invalid data array length: " + data.Length, "data");
            }

            if (m_fieldsMap == null)
            {
                throw new InvalidOperationException("Cannot invoke AddDocumentTypeDescriptor before BeginDefineDocumentTypes");
            }

            if (m_descriptor == null)
            {
                throw new InvalidOperationException("Cannot invoke AddDocumentTypeDescriptor after Commit was called");
            }

            var docType = m_descriptor.RequireDocumentTypeName(docTypeName);
            
            var fields = new FieldMetadata[data.Length / 2];
            for (var i = 0; i < fields.Length; i++)
            {
                fields[i] = new FieldMetadata(++m_lastFieldId, (string)data[i*2], (string)data[i*2], (DbType)data[i*2+1], docType);
            }

            var result = new DocumentTypeDescriptor(docTypeName, baseDatasetName ?? docTypeName, docType, primaryKeyFieldName, fields.Select(x => x.FieldId).ToArray());
            m_descriptor.AddDocumentTypeDescriptor(result);
            foreach (var field in fields)
            {
                m_descriptor.AddField(field);
            }
            return result;
        }

        public JoinDescriptor AddJoinDescriptor(int docType, Type type, params object[] data)
        {
            if (type == null)
            {
                throw new ArgumentNullException("type");
            }

            if (data.Length == 0)
            {
                throw new ArgumentException("Must have at least one property info", "data");
            }

            if (0 != data.Length % 2)
            {
                throw new ArgumentException("Invalid data array length: " + data.Length, "data");
            }

            if (m_fieldsMap == null)
            {
                throw new InvalidOperationException("Cannot invoke AddJoinDescriptor before BeginDefineDocumentTypes");
            }

            if (m_descriptor == null)
            {
                throw new InvalidOperationException("Cannot invoke AddJoinDescriptor after Commit was called");
            }

            var docTypeDescriptor = m_descriptor.RequireDocumentType(docType);

            var joinPropertyNameToDocumentType = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < data.Length; i += 2)
            {
                try
                {
                    var propName = data[i] as string;
                    if (string.IsNullOrEmpty(propName))
                    {
                        throw new ArgumentException("Empty property name in the data array, element " + i);
                    }

                    var propDocType = (int) data[i + 1];
                    if (null == m_descriptor.TryGetDocTypeName(propDocType))
                    {
                        throw new ArgumentException("Unknown document type " + propDocType + " for property " + propName);
                    }

                    joinPropertyNameToDocumentType.Add(propName, propDocType);
                }
                catch (Exception e)
                {
                    throw new ArgumentException("Failed to parse property infos: " + e.Message, "data", e);
                }
            }

            var result = new JoinDescriptor(docTypeDescriptor.DocumentType, type, joinPropertyNameToDocumentType);
            m_descriptor.AddJoinDescriptor(result);
            return result;
        }

        public void AddIdentifierAliases(string docTypeName, params object[] data)
        {
            if (data.Length == 0 || 0 != data.Length % 2)
            {
                throw new ArgumentException("Invalid data array length: " + data.Length, "data");
            }

            if (m_descriptor == null)
            {
                throw new InvalidOperationException("Cannot invoke AddDocumentTypeDescriptor after Commit was called");
            }

            var splitters = new[] {'.'};

            for (var i = 0; i < data.Length; i += 2)
            {
                var aliasData = data[i] as string;
                if (string.IsNullOrEmpty(aliasData))
                {
                    throw new ArgumentException("Empty alias, element " + i);
                }

                var alias = aliasData.Split(splitters, StringSplitOptions.RemoveEmptyEntries);
                
                var mappedValueData = data[i + 1] as string;
                if (string.IsNullOrEmpty(mappedValueData))
                {
                    throw new ArgumentException("Empty mapped value, element " + (i+1));
                }

                var mappedValue = mappedValueData.Split(splitters, StringSplitOptions.RemoveEmptyEntries);

                m_descriptor.AddIdentifierAlias(docTypeName, new List<string>(alias), mappedValue);
            }
        }

        public DataContainerDescriptor Commit()
        {
            if (m_descriptor == null)
            {
                throw new InvalidOperationException("Cannot invoke Commit more than once");
            }

            var result = m_descriptor;
            m_descriptor = null;
            return result;
        }

        private static Dictionary<int, Dictionary<DbType, int>> InitializeFieldsMap(IEnumerable<Tuple<int, string>> docTypes)
        {
            var result = new Dictionary<int, Dictionary<DbType, int>>();

            var types = (DbType[])Enum.GetValues(typeof(DbType));

            foreach (var pair in docTypes)
            {
                var dict = new Dictionary<DbType, int>(types.Length * 2);
                result.Add(pair.Item1, dict);
                foreach (var type in types)
                {
                    dict.Add(type, 0);
                }
            }

            return result;
        }
    }
}
