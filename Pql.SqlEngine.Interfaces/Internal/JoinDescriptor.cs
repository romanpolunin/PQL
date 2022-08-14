using System.Reflection;
using System.Text.Json.Serialization;

namespace Pql.SqlEngine.Interfaces.Internal
{
    public class JoinDescriptor: IJsonOnDeserialized
    {
        static JoinDescriptor()
        {
            FieldSetter = typeof(Dictionary<string, int>).GetField("comparer", BindingFlags.NonPublic | BindingFlags.Instance)
                ?? throw new Exception("Failed to reflect: " + nameof(FieldSetter));
        }

        protected static readonly FieldInfo FieldSetter;

        [JsonInclude]
        public readonly int DocumentType;

        /// <summary>
        /// Type information on the join class.
        /// </summary>
        [JsonIgnore]
        public readonly Type JoinClassType;

        /// <summary>
        /// Mapping of join property names to document types.
        /// </summary>
        [JsonInclude]
        public Dictionary<string, int> JoinPropertyNameToDocumentType;

        public JoinDescriptor()
        {
            (this as IJsonOnDeserialized).OnDeserialized();
        }

        public JoinDescriptor(int documentType, Type classType, Dictionary<string, int> joinPropertyNameToDocumentType)
        {
            if (documentType < 0)
            {
                throw new ArgumentException("Invalid document type", nameof(documentType));
            }

            if (joinPropertyNameToDocumentType == null)
            {
                throw new ArgumentNullException(nameof(joinPropertyNameToDocumentType));
            }

            if (!ReferenceEquals(joinPropertyNameToDocumentType.Comparer, StringComparer.OrdinalIgnoreCase))
            {
                throw new ArgumentException("Invalid comparer set for this dictionary", nameof(joinPropertyNameToDocumentType));
            }

            DocumentType = documentType;
            JoinClassType = classType ?? throw new ArgumentNullException(nameof(classType));
            JoinPropertyNameToDocumentType = joinPropertyNameToDocumentType;
        }

        void IJsonOnDeserialized.OnDeserialized()
        {
            if (JoinPropertyNameToDocumentType == null)
            {
                JoinPropertyNameToDocumentType = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            }
            else
            {
                // have to make sure that deserialized dictionary uses proper comparer
                // using this little dirty private field setter 
                FieldSetter.SetValue(JoinPropertyNameToDocumentType, StringComparer.OrdinalIgnoreCase);
            }
        }
    }
}