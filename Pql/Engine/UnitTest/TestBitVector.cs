using System;
using System.Collections;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Pql.ClientDriver.Protocol;

namespace Pql.Engine.UnitTest
{
    [TestClass]
    public class TestBitVector
    {
        private static readonly Random Rand = new Random(Environment.TickCount);

        [TestMethod]
        public void TestRandomValuesSetter()
        {
            TestRandomValuesSetter(0);
            TestRandomValuesSetter(1);
            TestRandomValuesSetter(2);
            TestRandomValuesSetter(3);
            TestRandomValuesSetter(4);
            TestRandomValuesSetter(5);
            TestRandomValuesSetter(8);
            TestRandomValuesSetter(15);
            TestRandomValuesSetter(17);
            TestRandomValuesSetter(31);
            TestRandomValuesSetter(33);
            TestRandomValuesSetter(4531);
        }

        [TestMethod]
        public void TestReadWrite()
        {
            TestReadWrite(0);
            TestReadWrite(1);
            TestReadWrite(2);
            TestReadWrite(3);
            TestReadWrite(4);
            TestReadWrite(5);
            TestReadWrite(8);
            TestReadWrite(15);
            TestReadWrite(16);
            TestReadWrite(17);
            TestReadWrite(31);
            TestReadWrite(32);
            TestReadWrite(33);
            TestReadWrite(4531);
        }

        private void TestReadWrite(int size)
        {
            var data = CreateRandomBoolArray(size);
            var vectorData1 = new int[BitVector.GetArrayLength(size)];
            for (var i = 0; i < data.Length; i++)
            {
                if (data[i])
                    BitVector.Set(vectorData1, i);
                else
                    BitVector.Clear(vectorData1, i);
            }

            var vectorData2 = new int[vectorData1.Length];

            using (var stream = new MemoryStream())
            {
                using (var writer = new BinaryWriter(stream, Encoding.Default, true))
                {
                    BitVector.Write(vectorData1, size, writer);
                }
                Assert.AreEqual(BitVector.GetByteCount(size), stream.Length);

                stream.Seek(0, SeekOrigin.Begin);
                using (var reader = new BinaryReader(stream, Encoding.Default, true))
                {
                    BitVector.Read(vectorData2, size, reader);
                }

                Assert.AreEqual(stream.Position, stream.Length);
                Assert.IsTrue(vectorData1.SequenceEqual(vectorData2));
            }
        }

        private void TestRandomValuesSetter(int size)
        {
            var data = CreateRandomBoolArray(size);
            var array = new BitArray(size);

            var vector = new int[BitVector.GetArrayLength(size)];

            // check both are false
            for (var i = 0; i < size; i++)
            {
                Assert.IsFalse(BitVector.Get(vector, i));
                Assert.IsFalse(BitVector.SafeGet(vector, i));
                Assert.IsFalse(array[i]);
            }

            // assign new values and check state immediately after assignment
            for (var i = 0; i < size; i++)
            {
                if (data[i])
                    BitVector.Set(vector, i);
                else
                    BitVector.Clear(vector, i);

                array[i] = data[i];
                Assert.AreEqual(array[i], BitVector.Get(vector, i));
                Assert.AreEqual(array[i], BitVector.SafeGet(vector, i));
            }

            // check all are equal after all setters
            for (var i = 0; i < size; i++)
            {
                Assert.AreEqual(array[i], BitVector.Get(vector, i));
                Assert.AreEqual(array[i], BitVector.SafeGet(vector, i));
            }

            // assign new values and check state immediately after assignment
            for (var i = 0; i < size; i++)
            {
                if (data[i])
                    BitVector.SafeSet(vector, i);
                else
                    BitVector.SafeClear(vector, i);

                array[i] = data[i];
                Assert.AreEqual(array[i], BitVector.Get(vector, i));
                Assert.AreEqual(array[i], BitVector.SafeGet(vector, i));
            }

            // check all are equal after all setters
            for (var i = 0; i < size; i++)
            {
                Assert.AreEqual(array[i], BitVector.Get(vector, i));
                Assert.AreEqual(array[i], BitVector.SafeGet(vector, i));
            }
        }

        public bool[] CreateRandomBoolArray(int size)
        {
            var result = new bool[size];
            for (var i = 0; i < size; i++)
            {
                result[i] = 0 == Rand.Next(1000) % 2;
            }

            return result;
        }
    }
}