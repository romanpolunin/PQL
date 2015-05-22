using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Pql.Engine.DataContainer.RamDriver;
using Pql.ExpressionEngine.Interfaces;

namespace Pql.Engine.UnitTest
{
    [TestClass]
    public class ExpandableArrayTest
    {
        [TestMethod]
        public void Test()
        {
            const int blockCount = 10;
            const int elemPerItem = 5;
            var itemsPerBlock = ExpandableArray<int>.ComputeItemsPerBlock(sizeof(int));

            var x = new ExpandableArray<int>(elemPerItem, sizeof(int));
            Assert.AreEqual(itemsPerBlock, x.ElementsPerBlock / elemPerItem);
            Assert.AreEqual(0, x.GetLocalIndex(0));
            Assert.AreEqual(1, x.GetLocalIndex(1));
            Assert.AreEqual(x.ElementsPerBlock-1, x.GetLocalIndex(x.ElementsPerBlock-1));
            Assert.AreEqual(0, x.GetLocalIndex(x.ElementsPerBlock));
            Assert.AreEqual(0, x.GetLocalIndex(itemsPerBlock*elemPerItem));
            Assert.AreEqual(0, x.GetLocalIndex(itemsPerBlock*blockCount*elemPerItem));
            Assert.AreEqual(2, x.GetLocalIndex(itemsPerBlock*blockCount*elemPerItem + 2));
            Assert.AreEqual(x.ElementsPerBlock-1, x.GetLocalIndex(itemsPerBlock*blockCount*elemPerItem + x.ElementsPerBlock-1));

            x.EnsureCapacity(itemsPerBlock*blockCount*elemPerItem + 1);
            var realBlockCount = x.EnumerateBlocks().Count();

            Assert.IsTrue(realBlockCount == 1 + blockCount + x.BlockCountIncrement);
        }

        [TestMethod]
        public void DebugUtil()
        {
            var set = new HashSet<string>();
            set.Add(null);
            Assert.IsFalse(set.Contains(string.Empty));
            Assert.IsTrue(set.Contains(null));
            set.Add(string.Empty);
            Assert.IsTrue(set.Contains(null));
            Assert.IsTrue(set.Contains(""));
            Assert.IsTrue(set.Contains(string.Empty));
            Assert.IsFalse(set.Add(""));

            var bs = new HashSet<SizableArrayOfByte>(SizableArrayOfByte.DefaultComparer.Instance);
            var empty = new SizableArrayOfByte();
            bs.Add(null);
            Assert.IsFalse(bs.Contains(empty));
            Assert.IsTrue(bs.Contains(null));
            bs.Add(new SizableArrayOfByte());
            Assert.IsTrue(bs.Contains(null));
            Assert.IsTrue(bs.Contains(empty));
            Assert.IsTrue(bs.Contains(new SizableArrayOfByte()));
            Assert.IsFalse(bs.Add(new SizableArrayOfByte()));
            Assert.IsFalse(bs.Add(empty));
        }
    }
}
