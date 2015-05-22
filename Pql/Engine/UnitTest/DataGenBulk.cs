using System;
using System.Collections.Generic;
using System.Data;
using Pql.ClientDriver;
using Pql.ClientDriver.Protocol;

namespace Pql.Engine.UnitTest
{
    public class DataGenBulk
    {
        private readonly RowData m_rowData;
        public string[] FieldNames { get; private set; }
        public int Count { get; private set; }
        public int FirstId { get; private set; }
        public bool UpdateMode;

        public DataGenBulk(int count, int firstId)
        {
            Count = count;
            FirstId = firstId;

            FieldNames = new[]
            {
                "id", "fieldByte1", "fieldGuid2", "fieldString3", "fieldBinary4", "fieldDecimal5", "fieldBool6",
                "fieldBool15", "fieldDecimal14"
            };
            m_rowData =
                new RowData(new[]
                {
                    DbType.Int64, DbType.Byte, DbType.Guid, DbType.String, DbType.Binary, DbType.Currency, DbType.Boolean,
                    DbType.Boolean, DbType.Decimal
                });
        }

        public IEnumerable<RowData> GetInsertEnumerableForPerformanceTest()
        {
            var random = new Random();
            m_rowData.BinaryData[0].SetLength(100);
            m_rowData.StringData[0].SetLength(10);

            var src = m_rowData.BinaryData[0].Data;
            random.NextBytes(m_rowData.BinaryData[0].Data);
            var dest = m_rowData.StringData[0].Data;
            for (var i = 0; i < dest.Length; i++)
            {
                dest[i] = (char)(' ' + (src[i] % 52));
            }

            for (var id = FirstId; id < FirstId + Count; id++)
            {
                BitVector.Set(m_rowData.NotNulls, 0);
                //BitVector.Set(m_rowData.NotNulls, 1);
                //BitVector.Set(m_rowData.NotNulls, 2);
                //BitVector.Set(m_rowData.NotNulls, 3);
                //BitVector.Set(m_rowData.NotNulls, 4);
                //BitVector.Set(m_rowData.NotNulls, 5);
                //BitVector.Set(m_rowData.NotNulls, 6);
                //BitVector.Set(m_rowData.NotNulls, 7);
                //BitVector.Set(m_rowData.NotNulls, 8);

                m_rowData.ValueData8Bytes[0].AsInt64 = id;
                //m_rowData.ValueData8Bytes[1].AsByte = (byte)(id % 256);
                //m_rowData.ValueData8Bytes[2].AsBoolean = random.Next(1) == 1;
                //m_rowData.ValueData8Bytes[3].AsBoolean = random.Next(1) == 1;
                //m_rowData.ValueData16Bytes[0].AsGuid = Guid.NewGuid();
                //m_rowData.ValueData16Bytes[1].AsDecimal = (decimal)random.NextDouble();
                //m_rowData.ValueData16Bytes[2].AsDecimal = (decimal)random.NextDouble();

                //random.NextBytes(m_rowData.BinaryData[0].Data);
                
                //if (UpdateMode)
                //{
                //    var src = id.ToString().ToCharArray();
                //    m_rowData.StringData[0].SetLength(src.Length);
                //    var dest = m_rowData.StringData[0];
                //    for (var i = 0; i < dest.Length; i++)
                //    {
                //        dest.Data[i] = src[i];
                //    }
                //}
                //else
                //{
                //var src = m_rowData.BinaryData[0].Data;
                //var dest = m_rowData.StringData[0].Data;
                //for (var i = 0; i < dest.Length; i++)
                //{
                //    dest[i] = (char)(' ' + (src[i] % 52));
                //}
                //}

                yield return m_rowData;
            }
        }

        public IEnumerable<RowData> GetInsertEnumerableForDemoData()
        {
            var random = new Random();
            m_rowData.BinaryData[0].SetLength(100);
            m_rowData.StringData[0].SetLength(10);

            var src = m_rowData.BinaryData[0].Data;
            random.NextBytes(m_rowData.BinaryData[0].Data);
            var dest = m_rowData.StringData[0].Data;
            for (var i = 0; i < dest.Length; i++)
            {
                dest[i] = (char)(' ' + (src[i] % 52));
            }

            for (var id = FirstId; id < FirstId + Count; id++)
            {
                BitVector.Set(m_rowData.NotNulls, 0);
                BitVector.Set(m_rowData.NotNulls, 1);
                BitVector.Set(m_rowData.NotNulls, 2);
                BitVector.Set(m_rowData.NotNulls, 3);
                BitVector.Set(m_rowData.NotNulls, 4);
                BitVector.Set(m_rowData.NotNulls, 5);
                BitVector.Set(m_rowData.NotNulls, 6);
                BitVector.Set(m_rowData.NotNulls, 7);
                BitVector.Set(m_rowData.NotNulls, 8);

                m_rowData.ValueData8Bytes[0].AsInt64 = id;
                m_rowData.ValueData8Bytes[1].AsByte = (byte)(id % 256);
                m_rowData.ValueData8Bytes[2].AsBoolean = random.Next(1) == 1;
                m_rowData.ValueData8Bytes[3].AsBoolean = random.Next(1) == 1;
                m_rowData.ValueData16Bytes[0].AsGuid = Guid.NewGuid();
                m_rowData.ValueData16Bytes[1].AsDecimal = (decimal)random.NextDouble();
                m_rowData.ValueData16Bytes[2].AsDecimal = (decimal)random.NextDouble();

                random.NextBytes(m_rowData.BinaryData[0].Data);

                yield return m_rowData;
            }
        }
    }
}