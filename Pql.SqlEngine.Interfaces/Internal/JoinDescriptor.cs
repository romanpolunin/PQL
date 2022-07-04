using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.Serialization;

namespace Pql.Engine.Interfaces.Internal
{
    [DataContract]
    public class JoinDescriptor
    {
        static JoinDescriptor()
        {
            FieldSetter = typeof(Dictionary<string, int>).GetField("comparer", BindingFlags.NonPublic | BindingFlags.Instance);
        }

        protected static readonly FieldInfo FieldSetter;

        [DataMember(IsRequired = true, EmitDefaultValue = true)]
        public readonly int DocumentType;

        /// <summary>
        /// Type information on the join class.
        /// </summary>
        [NonSerialized]
        public readonly Type JoinClassType;

        /// <summary>
        /// Mapping of join property names to document types.
        /// </summary>
        [DataMember(IsRequired = true, EmitDefaultValue = true)]
        public Dictionary<string, int> JoinPropertyNameToDocumentType;

        public JoinDescriptor()
        {
            OnDeserialized(new StreamingContext(StreamingContextStates.Other));
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

        [OnDeserialized]
        protected void OnDeserialized(StreamingContext streamingContext)
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