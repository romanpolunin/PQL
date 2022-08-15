using Pql.SqlEngine.Interfaces.Internal;

namespace Pql.SqlEngine.DataContainer.RamDriver
{
    internal sealed class DocumentDataContainerEnumerator_IndexScan : ADocumentDataContainerEnumeratorBase
    {
        private readonly SortIndex _sortIndex;
        private readonly bool _descending;

        public int PositionInIndex;

        public override bool MoveNext()
        {
            var orderData = _sortIndex.OrderData;
            var validCount = _sortIndex.ValidDocCount;
            var bmpList = DataContainer.ValidDocumentsBitmap;

            if (validCount > UntrimmedCount)
            {
                throw new Exception("Internal error: valid documents count may never be greater than untrimmed count");
            }

            var increment = _descending ? -1 : 1;
            do
            {
                // move at least one position forward,
                // also skip all deleted documents; their keys are replaced with null values
                PositionInIndex += increment;

                if (PositionInIndex >= validCount || PositionInIndex < 0)
                {
                    break;
                }

                Position = orderData[PositionInIndex];
            } while (!bmpList.SafeGet(Position));

            if (PositionInIndex >= validCount || PositionInIndex < 0)
            {
                HaveData = false;
            }
            else
            {
                HaveData = true;
                ReadRow();
            }
            return HaveData;
        }

        public DocumentDataContainerEnumerator_IndexScan(
            int untrimmedCount, 
            DriverRowData rowData, 
            DocumentDataContainer dataContainer,
            IReadOnlyList<FieldMetadata> fields,
            int countOfMainFields, 
            SortIndex sortIndex, 
            bool descending)
            : base(untrimmedCount, rowData, dataContainer, fields, countOfMainFields)
        {
            if (sortIndex == null)
            {
                throw new ArgumentNullException(nameof(sortIndex));
            }

            // note that we ignore value of sortIndex.IsValid here
            // that's because invalidation of index only happens when the data is stale
            // we only check state of an index and optionally update it in the beginning of processing pipeline
            if (sortIndex.OrderData == null || sortIndex.OrderData.Length > untrimmedCount)
            {
                throw new ArgumentException("Index on column is in invalid state", nameof(sortIndex));
            }

            _sortIndex = sortIndex;
            _descending = descending;
            PositionInIndex = descending ? _sortIndex.ValidDocCount : -1;

            ReadStructureAndTakeLocks();
        }
    }
}