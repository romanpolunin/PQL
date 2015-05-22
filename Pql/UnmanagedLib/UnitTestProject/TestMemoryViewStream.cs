using System;
using System.IO;
using System.Text;
using Pql.UnmanagedLib;

namespace Pql.UnitTestProject
{
    public class TestMemoryViewStream
    {
        public unsafe void Test()
        {
            var pool = new DynamicMemoryPool();

            uint blocksize = 234401;
            var pblock = (byte*)pool.Alloc(blocksize);
            var stream = new MemoryViewStream();
            stream.Attach(pblock, blocksize);
            
            
            var data = new byte[blocksize];
            for (var i = 0; i < data.Length; i++)
            {
                data[i] = (byte)i;
            }

            using (var writer = new BinaryWriter(stream, Encoding.UTF8, true))
            {
                writer.Write(data, 0, data.Length);
            }

            if (stream.Position != stream.Length)
            {
                throw new Exception("Invalid position: " + stream.Position);
            }

            stream.Seek(0, SeekOrigin.Begin);

            if (stream.Position != 0)
            {
                throw new Exception("Invalid position: " + stream.Position);
            }

            using (var reader = new BinaryReader(stream, Encoding.UTF8, true))
            {
                for (var i = 0; i < data.Length; i++)
                {
                    //if (data[i] != *(pblock + i)) 
                    if (data[i] != reader.ReadByte())
                    {
                        throw new Exception("Failed at " + i);
                    }
                }
            }

            return;

            if (stream.Position != stream.Length)
            {
                throw new Exception("Invalid position: " + stream.Position);
            }

            stream.Position = 0;
        }
    }
}