using System.Text.Json.Serialization;

namespace Pql.SqlEngine.Interfaces.Internal
{
    public class DocumentTypeDescriptor: IJsonOnDeserialized, IJsonOnSerializing
    {
        [JsonInclude]
        public readonly string Name;

        [JsonInclude]
        public readonly string BaseDatasetName;

        [JsonInclude]
        public readonly string? PrimaryKeyFieldName;

        [JsonInclude]
        public readonly int DocumentType;

        [JsonInclude]
        public readonly int[] Fields;

        [JsonInclude]
        private Tuple<string[], string[]>[]? _identifierAliasesData;

        private Dictionary<List<string>, string[]> _identifierAliases;

        private DocumentTypeDescriptor()
        {
        }

        public DocumentTypeDescriptor(string name, string baseDatasetName, int docType, int[] fields) 
        : this(name, baseDatasetName, docType, null, fields) {}

        public DocumentTypeDescriptor(string name, string baseDatasetName, int docType, string? primaryKeyFieldName, int[] fields)
        {
            Name = name;
            BaseDatasetName = baseDatasetName;
            DocumentType = docType;
            Fields = (int[])fields.Clone();
            PrimaryKeyFieldName = primaryKeyFieldName;

            (this as IJsonOnDeserialized).OnDeserialized();
        }

        [JsonIgnore]
        public int FieldCount => Fields.Length;

        public bool TryGetIdentifierAlias(List<string> alias, out string[] mapped) => _identifierAliases.TryGetValue(alias, out mapped);

        void IJsonOnSerializing.OnSerializing()
        {
            _identifierAliasesData =
                _identifierAliases.Select(x => new Tuple<string[], string[]>(x.Key.ToArray(), x.Value)).ToArray();
        }

        void IJsonOnDeserialized.OnDeserialized()
        {
            if (string.IsNullOrEmpty(Name))
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

            _identifierAliases = new Dictionary<List<string>, string[]>(new IdentifierAliasComparer());

            if (_identifierAliasesData != null)
            {
                foreach (var tuple in _identifierAliasesData)
                {
                    _identifierAliases.Add(new List<string>(tuple.Item1), tuple.Item2);
                }

                // now discard serialization data
                _identifierAliasesData = null;
            }
        }

        public void AddIdentifierAlias(List<string> alias, string[] mapped)
        {
            if (alias == null || alias.Count == 0)
            {
                throw new ArgumentException("Alias must be not-null and have at least one part", nameof(alias));
            }

            if (mapped == null || mapped.Length == 0)
            {
                throw new ArgumentException("Mapped must be not-null and have at least one part", nameof(mapped));
            }

            if (_identifierAliases.ContainsKey(alias))
            {
                throw new ArgumentException("Duplicate alias", nameof(alias));
            }

            _identifierAliases.Add(alias, mapped);
        }

        public IEnumerable<Tuple<IReadOnlyList<string>, IReadOnlyCollection<string>>> EnumerateIdentiferAliases()
        {
            foreach (var pair in _identifierAliases)
            {
                yield return new Tuple<IReadOnlyList<string>, IReadOnlyCollection<string>>(pair.Key, pair.Value);
            }
        }

        public class IdentifierAliasComparer : IEqualityComparer<List<string>>
        {
            private static readonly StringComparer s_comparer = StringComparer.OrdinalIgnoreCase;

            public bool Equals(List<string>? x, List<string>? y)
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
                    if (!s_comparer.Equals(x[i], y[i]))
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
                    result = (result * 31) + s_comparer.GetHashCode(x);
                }

                return result;
            }
        }
    }
}