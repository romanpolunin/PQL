using System.Collections.Generic;
using Pql.Engine.Interfaces.Internal;

namespace Pql.Engine.DataContainer.RamDriver
{
    internal sealed class DocumentDataContainerEnumerator_FullScan : DocumentDataContainerEnumeratorBase
    {
        public override bool MoveNext()
        {
            if (Position >= UntrimmedCount)
            {
                return false;
            }

            var bmpList = DataContainer.ValidDocumentsBitmap;

            do
            {
                // move at least one position forward,
                // also skip all deleted documents; their keys are replaced with null values
                Position++;

                if (Position >= UntrimmedCount)
                {
                    break;
                }
            } while (!bmpList.SafeGet(Position));

            HaveData = Position < UntrimmedCount;
            if (HaveData)
            {
                ReadRow();
            }
            return HaveData;
        }

        public DocumentDataContainerEnumerator_FullScan(
            int untrimmedCount, 
            DriverRowData rowData, 
            DocumentDataContainer dataContainer, 
            IReadOnlyList<FieldMetadata> fields,
            int countOfMainFields)
            : base(untrimmedCount, rowData, dataContainer, fields, countOfMainFields)
        {
            ReadStructureAndTakeLocks();
        }
    }
}