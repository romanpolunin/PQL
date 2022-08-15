using Pql.SqlEngine.Interfaces.Internal;
using Pql.SqlEngine.Interfaces.Services;

namespace Pql.SqlEngine.DataContainer.RamDriver
{
    internal class DocumentDataContainerEnumerator_BulkPkScan : ADocumentDataContainerEnumeratorBase
    {
        private readonly IDriverDataEnumerator _inputEnumerator;

        public override void FetchAdditionalFields()
        {
            // do nothing, all fields are being fetched into DriverRow by underlying input bulk data enumerator
        }

        public override bool MoveNext()
        {
            var bmpList = DataContainer.ValidDocumentsBitmap;

            // scroll forward on input data until another matching document is found
            while (_inputEnumerator.MoveNext())
            {
                var entityId = _inputEnumerator.Current.InternalEntityId;
                // document must exist and be valid (not deleted)
                if (DataContainer.DocumentIdToIndex.TryGetValueInt32(entityId, ref Position))
                {
                    if (bmpList.SafeGet(Position))
                    {
                        HaveData = true;
                        return true;
                    }
                }
            }

            Position = -1;
            HaveData = false;
            return false;
        }

        public DocumentDataContainerEnumerator_BulkPkScan(
            int untrimmedCount, DriverRowData rowData, DocumentDataContainer dataContainer, List<FieldMetadata> fields, IDriverDataEnumerator inputDataEnumerator)
            :
                base(untrimmedCount, rowData, dataContainer, fields, fields.Count - 1)
        {
            _inputEnumerator = inputDataEnumerator ?? throw new ArgumentNullException(nameof(inputDataEnumerator));
            
            ReadStructureAndTakeLocks();
        }
    }
}