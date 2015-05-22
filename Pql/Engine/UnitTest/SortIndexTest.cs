using System.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Pql.Engine.DataContainer.RamDriver;
using Pql.UnmanagedLib;

namespace Pql.Engine.UnitTest
{
    [TestClass]
    public class SortIndexTest
    {
        static readonly IUnmanagedAllocator Pool = new DynamicMemoryPool();

        [TestMethod]
        public void TestIndexInt32()
        {
            using (var validDocsBitmap = new BitVector(Pool))
            {
                validDocsBitmap.EnsureCapacity(4);

                validDocsBitmap.ChangeAll(true);

                using (var data = new ColumnData<int>(DbType.Int32, Pool))
                {
                    data.EnsureCapacity(4);

                    data.DataArray.GetBlock(0)[0] = 1;
                    data.DataArray.GetBlock(0)[1] = 3;
                    data.DataArray.GetBlock(0)[2] = -1;
                    data.DataArray.GetBlock(0)[3] = 555;

                    data.NotNulls.Set(0);
                    data.NotNulls.Set(1);
                    data.NotNulls.Set(2);
                    data.NotNulls.Clear(3);

                    var index = new SortIndex();
                    index.Update(data, validDocsBitmap, 4);

                    Assert.AreEqual(4, index.OrderData.Length);

                    // expected resulting order: 3, 2, 0, 1
                    // item 3 goes on top because it is marked as NULL
                    Assert.AreEqual(3, index.OrderData[0]);
                    Assert.AreEqual(2, index.OrderData[1]);
                    Assert.AreEqual(0, index.OrderData[2]);
                    Assert.AreEqual(1, index.OrderData[3]);
                }
            }
        }

        [TestMethod]
        public void TestIndexByte()
        {
            using (var validDocsBitmap = new UnmanagedLib.BitVector(Pool))
            {
                validDocsBitmap.EnsureCapacity(1);
                validDocsBitmap.ChangeAll(true);

                using (var data = new ColumnData<byte>(DbType.Byte, Pool))
                {
                    data.EnsureCapacity(4);

                    data.DataArray.GetBlock(0)[0] = 1;
                    data.DataArray.GetBlock(0)[1] = 3;
                    data.DataArray.GetBlock(0)[2] = 0;
                    data.DataArray.GetBlock(0)[3] = 255;

                    data.NotNulls.Set(0);
                    data.NotNulls.Set(1);
                    data.NotNulls.Set(2);
                    data.NotNulls.Clear(3);

                    var index = new SortIndex();
                    index.Update(data, validDocsBitmap, 4);

                    Assert.AreEqual(4, index.OrderData.Length);

                    // expected resulting order: 3, 2, 0, 1
                    // item 3 goes on top because it is marked as NULL
                    Assert.AreEqual(3, index.OrderData[0]);
                    Assert.AreEqual(2, index.OrderData[1]);
                    Assert.AreEqual(0, index.OrderData[2]);
                    Assert.AreEqual(1, index.OrderData[3]);
                }
            }
        }
    }
}
