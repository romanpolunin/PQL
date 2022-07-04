using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;

namespace Pql.Engine.Interfaces.Internal
{
    [DataContract]
    public class DataContainerDescriptor
    {
        static DataContainerDescriptor()
        {
            // after deserialization, have to make sure that the deserialized dictionary uses proper case-insensitive comparer
            FieldSetterStringInt = typeof(Dictionary<string, int>).GetField("comparer", BindingFlags.NonPublic | BindingFlags.Instance);
        }

        [DataMember(IsRequired = true, EmitDefaultValue = true)]
        protected Dictionary<string, int> DocTypeNameToDocType;

        [DataMember(IsRequired = true, EmitDefaultValue = true)]
        protected Dictionary<int, JoinDescriptor> JoinDescriptors;

        [DataMember(IsRequired = true, EmitDefaultValue = true)]
        protected Dictionary<int, DocumentTypeDescriptor> DocumentTypeDescriptors;

        [DataMember(IsRequired = true, EmitDefaultValue = true)]
        protected Dictionary<int, FieldMetadata> Fields;

        [IgnoreDataMember]
        private Dictionary<int, Dictionary<string, int>> DocTypeFieldNameToFieldId;
        private static readonly FieldInfo FieldSetterStringInt;

        public DataContainerDescriptor()
        {
            OnDeserialized(new StreamingContext(StreamingContextStates.Other));
        }

        public FieldMetadata RequireField(int fieldId)
        {
            if (!Fields.TryGetValue(fieldId, out var result))
            {
                throw new ArgumentException("Invalid fieldId: " + fieldId);
            }

            return result;
        }

        public FieldMetadata RequireField(int docType, string fieldName)
        {
            var result = TryGetField(docType, fieldName);
            if (result == null)
            {
                throw new ArgumentException(string.Format(
                    "Invalid combination of document type {0} and field name {1}", docType, fieldName));
            }

            return result;
        }

        public FieldMetadata TryGetField(int docType, string fieldName)
        {
            if (!DocTypeFieldNameToFieldId.TryGetValue(docType, out var docFields))
            {
                return null;
            }

            if (!docFields.TryGetValue(fieldName, out var fieldId))
            {
                return null;
            }

            // have to call Require here, otherwise we'd allow for inconsistent state
            return RequireField(fieldId);
        }

        [OnDeserialized]
        protected void OnDeserialized(StreamingContext streamingContext)
        {
            if (DocTypeNameToDocType == null)
            {
                DocTypeNameToDocType = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            }
            else
            {
                // have to make sure that deserialized dictionary uses proper comparer
                // using this little dirty private field setter 
                FieldSetterStringInt.SetValue(DocTypeNameToDocType, StringComparer.OrdinalIgnoreCase);
            }

            if (JoinDescriptors == null)
            {
                JoinDescriptors = new Dictionary<int, JoinDescriptor>();
            }

            if (DocumentTypeDescriptors == null)
            {
                DocumentTypeDescriptors = new Dictionary<int, DocumentTypeDescriptor>();
            }

            if (Fields == null)
            {
                Fields = new Dictionary<int, FieldMetadata>();
            }

            DocTypeFieldNameToFieldId = new Dictionary<int, Dictionary<string, int>>();
            foreach (var docType in DocumentTypeDescriptors)
            {
                var dict = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                foreach (var fieldId in docType.Value.Fields)
                {
                    dict.Add(RequireField(fieldId).Name, fieldId);
                }
                
                DocTypeFieldNameToFieldId.Add(docType.Key, dict);
            }

            Validate();
        }

        public void Validate()
        {
            if (JoinDescriptors == null || DocTypeNameToDocType == null || DocumentTypeDescriptors == null)
            {
                throw new InvalidOperationException("Some internal members are not initialized");
            }

            if (DocTypeNameToDocType.Count != DocumentTypeDescriptors.Count
                || DocTypeNameToDocType.Values.Any(x => !DocumentTypeDescriptors.ContainsKey(x)))
            {
                throw new InvalidOperationException("Document types data does not match document type names mapping");
            }

            if (JoinDescriptors.Keys.Any(x => !DocumentTypeDescriptors.ContainsKey(x)))
            {
                throw new InvalidOperationException("Join descriptors data does not match document types data");
            }

            foreach (var docType in DocumentTypeDescriptors.Values)
            {
                if (docType.PrimaryKeyFieldName != null)
                {
                    RequireField(docType.DocumentType, docType.PrimaryKeyFieldName);
                }
                
                foreach (var fieldId in docType.Fields)
                {
                    RequireField(fieldId);
                }

                foreach (var pair in docType.EnumerateIdentiferAliases())
                {
                    var key = pair.Item1;
                    var value = pair.Item2;
                    if (key == null || value == null || key.Count == 0 || value.Count == 0)
                    {
                        throw new InvalidOperationException("One of the identifer alias mappings has empty key or value");
                    }

                    foreach (var x in key)
                    {
                        if (string.IsNullOrEmpty(x))
                        {
                            throw new InvalidOperationException("One of the identifier alias mappings has empty component in alias");
                        }
                    }

                    foreach (var x in value)
                    {
                        if (string.IsNullOrEmpty(x))
                        {
                            throw new InvalidOperationException("One of the identifier alias mappings has empty component in mapped value");
                        }
                    }
                }
            }
        }

        public string TryGetDocTypeName(int docType)
        {
            foreach (var pair in DocTypeNameToDocType)
            {
                if (pair.Value == docType)
                {
                    return pair.Key;
                }
            }

            return null;
        }

        public DocumentTypeDescriptor RequireDocumentType(int docType)
        {
            if (!DocumentTypeDescriptors.TryGetValue(docType, out var result))
            {
                throw new ArgumentException("Unknown document type: " + docType);
            }

            return result;
        }

        public DocumentTypeDescriptor RequireDocumentType(string docTypeName)
        {
            return RequireDocumentType(RequireDocumentTypeName(docTypeName));
        }

        public IEnumerable<DocumentTypeDescriptor> EnumerateDocumentTypes()
        {
            return DocumentTypeDescriptors.Values;
        }

        public IEnumerable<FieldMetadata> EnumerateFields()
        {
            return Fields.Values;
        }

        public IEnumerable<Tuple<int, string>> EnumerateDocumentTypeNames()
        {
            return DocTypeNameToDocType.Select(x => new Tuple<int, string>(x.Value, x.Key));
        }

        public void AddField(FieldMetadata field)
        {
            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            if (Fields.ContainsKey(field.FieldId))
            {
                throw new ArgumentException("Duplicate field id: " + field.FieldId, nameof(field));
            }

            var docType = field.OwnerDocumentType;

            if (!DocTypeFieldNameToFieldId.TryGetValue(field.OwnerDocumentType, out var docFields))
            {
                docFields = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                DocTypeFieldNameToFieldId.Add(field.OwnerDocumentType, docFields);
            }

            if (docFields.ContainsKey(field.Name))
            {
                throw new ArgumentException(
                    string.Format("Entity {0} already has another field with name {1}", docType, field.Name));
            }

            Fields.Add(field.FieldId, field);
            docFields.Add(field.Name, field.FieldId);
        }

        public void AddIdentifierAlias(string docTypeName, List<string> alias, string[] mapped)
        {
            RequireDocumentType(RequireDocumentTypeName(docTypeName)).AddIdentifierAlias(alias, mapped);
        }

        public void AddDocumentTypeName(string name)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            if (DocTypeNameToDocType.ContainsKey(name))
            {
                throw new ArgumentException("Already have this document type name: " + name);
            }

            var docType = DocTypeNameToDocType.Count == 0 ? 1 : 1 + DocTypeNameToDocType.Values.Max();
            DocTypeNameToDocType.Add(name, docType);
        }

        public void AddDocumentTypeDescriptor(DocumentTypeDescriptor docType)
        {
            if (docType == null)
            {
                throw new ArgumentNullException(nameof(docType));
            }

            var existing = RequireDocumentTypeName(docType.Name);
            if (docType.DocumentType != existing)
            {
                throw new ArgumentException(string.Format(
                    "Supplied document type id {0} does not match existing id {1} for document type {2}",
                    docType.DocumentType, existing, docType.Name));
            }
            DocumentTypeDescriptors.Add(docType.DocumentType, docType);
        }

        public void AddJoinDescriptor(JoinDescriptor joinType)
        {
            if (joinType == null)
            {
                throw new ArgumentNullException(nameof(joinType));
            }

            JoinDescriptors.Add(joinType.DocumentType, joinType);
        }

        public int RequireDocumentTypeName(string docTypeName)
        {
            if (string.IsNullOrEmpty(docTypeName))
            {
                throw new ArgumentNullException(nameof(docTypeName));
            }

            if (!DocTypeNameToDocType.TryGetValue(docTypeName, out var result))
            {
                throw new ArgumentException("Unknown document type name: " + docTypeName, nameof(docTypeName));
            }

            return result;
        }
    }
}