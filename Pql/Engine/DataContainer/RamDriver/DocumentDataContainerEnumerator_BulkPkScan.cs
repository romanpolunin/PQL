using System;
using System.Collections.Generic;
using Pql.Engine.Interfaces.Internal;
using Pql.Engine.Interfaces.Services;

namespace Pql.Engine.DataContainer.RamDriver
{
    internal class DocumentDataContainerEnumerator_BulkPkScan : DocumentDataContainerEnumeratorBase
    {
        private readonly IDriverDataEnumerator m_inputEnumerator;

        public override void FetchAdditionalFields()
        {
            // do nothing, all fields are being fetched into DriverRow by underlying input bulk data enumerator
        }

        public override bool MoveNext()
        {
            var bmpList = DataContainer.ValidDocumentsBitmap;

            // scroll forward on input data until another matching document is found
            while (m_inputEnumerator.MoveNext())
            {
                var entityId = m_inputEnumerator.Current.InternalEntityId;
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
            m_inputEnumerator = inputDataEnumerator ?? throw new ArgumentNullException("inputDataEnumerator");
            
            ReadStructureAndTakeLocks();
        }
    }
}