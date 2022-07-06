using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Pql.ClientDriver.Tests
{
    [TestClass]
    public class SpanReaderWriterTests
    {
        [TestMethod]
        public void TestPrimitives()
        {
            var data = new byte[1000];
            var span = data.AsSpan();

            var writer = new SpanWriter(span);

            writer.Write(true);
            writer.Write(false);
            writer.Write(Int16.MinValue);
            writer.Write(Int16.MaxValue);
            writer.Write(UInt16.MinValue);
            writer.Write(UInt16.MaxValue);
            writer.Write(Int32.MinValue);
            writer.Write(Int32.MaxValue);
            writer.Write(UInt32.MinValue);
            writer.Write(UInt32.MaxValue);
            writer.Write(Int64.MinValue);
            writer.Write(Int64.MaxValue);
            writer.Write(UInt64.MinValue);
            writer.Write(UInt64.MaxValue);

            var reader = new SpanReader(span);

            Assert.AreEqual(true, reader.ReadBool());
            Assert.AreEqual(false, reader.ReadBool());
            Assert.AreEqual(Int16.MinValue, reader.ReadInt16());
            Assert.AreEqual(Int16.MaxValue, reader.ReadInt16());
            Assert.AreEqual(UInt16.MinValue, reader.ReadUInt16());
            Assert.AreEqual(UInt16.MaxValue, reader.ReadUInt16());
            Assert.AreEqual(Int32.MinValue, reader.ReadInt32());
            Assert.AreEqual(Int32.MaxValue, reader.ReadInt32());
            Assert.AreEqual(UInt32.MinValue, reader.ReadUInt32());
            Assert.AreEqual(UInt32.MaxValue, reader.ReadUInt32());
            Assert.AreEqual(Int64.MinValue, reader.ReadInt64());
            Assert.AreEqual(Int64.MaxValue, reader.ReadInt64());
            Assert.AreEqual(UInt64.MinValue, reader.ReadUInt64());
            Assert.AreEqual(UInt64.MaxValue, reader.ReadUInt64());
        }
    }
}