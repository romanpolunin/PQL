using System;

namespace Pql.ExpressionEngine.UnitTest
{
    public class SomeDataObject
    {
        public long Id;
        public string Name;
        public string Number;
        public int Status;
        public string[] StringValues;
    }

    public class TestData
    {
        public DateTime[] DateTimeArrayField1;
        public DateTime DateTimeField1;
        public DateTime DateTimeField2;
        public double DoubleField1;
        public double DoubleField2;
        public int Int64Field1;
        public int Int64Field2;
        public string StringField1;
        public string StringField2;
        public TestData[] TestDataArrayField1;

        public Func<int> AtomFunc = () => 1;
        public Func<int, int> UnaryFunc = x => x;
        public Func<int, int, int> BinaryFunc = (x, y) => x + y;

        public Int64 MyAtomFunc()
        {
            return Int64Field1;
        }

        public Int64 MyUnaryFunc(Int64 x)
        {
            return x;
        }

        public Int64 MyBinaryFunc(Int64 x, Int64 y)
        {
            return x + y;
        }
    }
}