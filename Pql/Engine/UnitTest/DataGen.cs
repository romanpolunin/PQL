using System.Collections.Generic;
using System.Data;
using System.Linq;

using Pql.Engine.Interfaces.Internal;

namespace Pql.Engine.UnitTest
{
    public static class DataGen
    {
        public static DataContainerDescriptor BuildContainerDescriptor()
        {
            var result = new DataContainerDescriptor();
            result.AddDocumentTypeName("testDoc");

            var testDocId = result.RequireDocumentTypeName("testDoc");

            var fieldId = 1;
            for (var i = 0; i < 2; i++)
            {
                result.AddField(new FieldMetadata(fieldId, "FieldByte" + fieldId, "Byte" + fieldId, DbType.Byte, testDocId));
                fieldId++;
                result.AddField(new FieldMetadata(fieldId, "FieldGuid" + fieldId, "Guid" + fieldId, DbType.Guid, testDocId));
                fieldId++;
                result.AddField(new FieldMetadata(fieldId, "FieldString" + fieldId, "String" + fieldId, DbType.String, testDocId));
                fieldId++;
                result.AddField(new FieldMetadata(fieldId, "FieldBinary" + fieldId, "Binary" + fieldId, DbType.Binary, testDocId));
                fieldId++;
                result.AddField(new FieldMetadata(fieldId, "FieldDecimal" + fieldId, "Decimal" + fieldId, DbType.Decimal, testDocId));
                fieldId++;
                result.AddField(new FieldMetadata(fieldId, "FieldBool" + fieldId, "Bool" + fieldId, DbType.Boolean, testDocId));
                fieldId++;
                result.AddField(new FieldMetadata(fieldId, "FieldDate" + fieldId, "Date" + fieldId, DbType.DateTime, testDocId));
                fieldId++;
                result.AddField(new FieldMetadata(fieldId, "FieldTime" + fieldId, "Time" + fieldId, DbType.Time, testDocId));
                fieldId++;
                result.AddField(new FieldMetadata(fieldId, "FieldDateTimeOffset" + fieldId, "DateTimeOffset" + fieldId, DbType.DateTimeOffset, testDocId));
                fieldId++;
            }

            result.AddField(new FieldMetadata(fieldId + 1, "id", "primary key", DbType.Int64, testDocId));

            var fieldIds = result.EnumerateFields().Select(x => x.FieldId).ToArray();
            result.AddDocumentTypeDescriptor(new DocumentTypeDescriptor("testDoc", "testDoc", 1, "id", fieldIds));

            result.AddIdentifierAlias("testDoc", new List<string> { "id", "ALIAS1" }, new[] { "id" });

            return result;
        }


    }
}
