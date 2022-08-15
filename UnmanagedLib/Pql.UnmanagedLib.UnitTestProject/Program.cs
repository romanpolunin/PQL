using System.Runtime.Serialization;

namespace Pql.UnitTestProject
{
    class Program
    {
        public static void Main()
        {
            //new TestBitVector().TestRandomValuesSetter();
            //new TestMemoryViewStream().Test();
            new TestConcurrentHashmapOfKeys().Test();
            //new TestConcurrentDictOfKeys().Test();
        }
    }

    [Serializable]
    public class MyException : Exception
    {
        public MyException() {}

        public MyException(string message) : base(message) {}

        public MyException(string message, Exception inner) : base(message, inner) {}

        protected MyException(
            SerializationInfo info,
            StreamingContext context) : base(info, context) {}
    }
}
