using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

namespace Pql.Engine.Interfaces.Internal
{
    [DataContract]
    public class DocumentTypeDescriptor
    {
        [DataMember(IsRequired = true, EmitDefaultValue = true)]
        public readonly string Name;

        [DataMember(IsRequired = true, EmitDefaultValue = true)]
        public readonly string BaseDatasetName;

        [DataMember(IsRequired = false, EmitDefaultValue = true)]
        public readonly string PrimaryKeyFieldName;

        [DataMember(IsRequired = true, EmitDefaultValue = true)]
        public readonly int DocumentType;

        [DataMember(IsRequired = true, EmitDefaultValue = true)]
        public readonly int[] Fields;

        [DataMember(IsRequired = false, EmitDefaultValue = true)]
        private Tuple<string[], string[]>[] m_identifierAliasesData;

        [IgnoreDataMember]
        private Dictionary<List<string>, string[]> m_identifierAliases;

        private DocumentTypeDescriptor()
        {
        }

        public DocumentTypeDescriptor(string name, string baseDatasetName, int docType, int[] fields) 
        : this(name, baseDatasetName, docType, null, fields) {}

        public DocumentTypeDescriptor(string name, string baseDatasetName, int docType, string primaryKeyFieldName, int[] fields)
        {
            Name = name;
            BaseDatasetName = baseDatasetName;
            DocumentType = docType;
            Fields = (int[])fields.Clone();
            PrimaryKeyFieldName = primaryKeyFieldName;

            OnDeserialized(new StreamingContext(StreamingContextStates.Other));
        }

        [IgnoreDataMember]
        public int FieldCount
        {
            get { return Fields.Length; }
        }

        public bool TryGetIdentifierAlias(List<string> alias, out string[] mapped)
        {
            return m_identifierAliases.TryGetValue(alias, out mapped);
        }

        [OnSerializing]
        protected void OnSerializing(StreamingContext streamingContext)
        {
            m_identifierAliasesData =
                m_identifierAliases == null
                    ? null
                    : m_identifierAliases.Select(x => new Tuple<string[], string[]>(x.Key.ToArray(), x.Value)).ToArray();
        }

        [OnDeserialized]
        protected void OnDeserialized(StreamingContext streamingContext)
        {
            if (String.IsNullOrEmpty(Name))
            {
                throw new InvalidOperationException("Name was deserialized as empty value");
            }

            if (PrimaryKeyFieldName != null && PrimaryKeyFieldName.Length == 0)
            {
                throw new InvalidOperationException("PrimaryKeyFieldValue was deserialized as non-null, but empty value");
            }
            
            if (DocumentType <= 0)
            {
                throw new InvalidOperationException("docType must be positive");
            }

            if (Fields == null)
            {
                throw new InvalidOperationException("Fields were deserialized as null array");
            }

            m_identifierAliases = new Dictionary<List<string>, string[]>(new IdentifierAliasComparer());

            if (m_identifierAliasesData != null)
            {
                foreach (var tuple in m_identifierAliasesData)
                {
                    m_identifierAliases.Add(new List<string>(tuple.Item1), tuple.Item2);
                }

                // now discard serialization data
                m_identifierAliasesData = null;
            }
        }

        public void AddIdentifierAlias(List<string> alias, string[] mapped)
        {
            if (alias == null || alias.Count == 0)
            {
                throw new ArgumentException("Alias must be not-null and have at least one part", "alias");
            }

            if (mapped == null || mapped.Length == 0)
            {
                throw new ArgumentException("Mapped must be not-null and have at least one part", "mapped");
            }

            if (m_identifierAliases.ContainsKey(alias))
            {
                throw new ArgumentException("Duplicate alias", "alias");
            }

            m_identifierAliases.Add(alias, mapped);
        }

        public IEnumerable<Tuple<IReadOnlyList<string>, IReadOnlyCollection<string>>> EnumerateIdentiferAliases()
        {
            foreach (var pair in m_identifierAliases)
            {
                yield return new Tuple<IReadOnlyList<string>, IReadOnlyCollection<string>>(pair.Key, pair.Value);
            }
        }

        public class IdentifierAliasComparer : IEqualityComparer<List<string>>
        {
            private static readonly StringComparer Comparer = StringComparer.OrdinalIgnoreCase;

            public bool Equals(List<string> x, List<string> y)
            {
                if (ReferenceEquals(x, y))
                {
                    return true;
                }

                if (x == null || y == null || x.Count != y.Count)
                {
                    return false;
                }

                for (var i = 0; i < x.Count; i++)
                {
                    if (!Comparer.Equals(x[i], y[i]))
                    {
                        return false;
                    }
                }

                return true;
            }

            public int GetHashCode(List<string> obj)
            {
                if (obj == null || obj.Count == 0)
                {
                    return 0;
                }

                int result = 0;
                foreach (var x in obj)
                {
                    result = result * 31 + Comparer.GetHashCode(x);
                }

                return result;
            }
        }
    }
}