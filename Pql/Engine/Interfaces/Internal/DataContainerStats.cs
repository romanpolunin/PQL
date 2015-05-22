using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Pql.Engine.Interfaces.Internal
{
    [DataContract]
    public class DataContainerStats
    {
        [DataMember]
        protected Dictionary<int, int> DocumentCounts;

        public DataContainerStats()
        {
            OnDeserialized(new StreamingContext(StreamingContextStates.Other));
        }

        private void OnDeserialized(StreamingContext streamingContext)
        {
            if (DocumentCounts == null)
            {
                DocumentCounts = new Dictionary<int, int>();
            }
        }

        public void SetDocumentCount(int documentType, int count)
        {
            if (count == 0)
            {
                DocumentCounts.Remove(documentType);
            }
            else
            {
                DocumentCounts[documentType] = count;
            }
        }

        public int TryGetDocumentCount(int documentType)
        {
            int result;
            return DocumentCounts != null && DocumentCounts.TryGetValue(documentType, out result) ? result : 0;
        }
    }
}