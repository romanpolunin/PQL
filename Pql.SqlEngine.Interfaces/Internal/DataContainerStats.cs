using System.Text.Json.Serialization;

namespace Pql.SqlEngine.Interfaces.Internal
{
    public class DataContainerStats: IJsonOnDeserialized
    {
        [JsonInclude]
        protected Dictionary<int, int> DocumentCounts;

        public DataContainerStats()
        {
            (this as IJsonOnDeserialized).OnDeserialized();
        }

        void IJsonOnDeserialized.OnDeserialized()
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

        public int TryGetDocumentCount(int documentType) => DocumentCounts != null && DocumentCounts.TryGetValue(documentType, out var result) ? result : 0;
    }
}