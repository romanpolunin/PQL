using System;
using System.Data;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Pql.Engine.Interfaces.Internal;
using Pql.ExpressionEngine.Interfaces;
using Pql.UnmanagedLib;

namespace Pql.Engine.DataContainer.RamDriver
{
    internal abstract class ColumnDataBase : IDisposable
    {
        private volatile Task[] m_dataLoaderTasks;

        public BitVector NotNulls;
        private bool m_disposed;

        protected ColumnDataBase(IUnmanagedAllocator allocator)
        {
            NotNulls = new BitVector(allocator);
        }

        protected ColumnDataBase(ColumnDataBase source, IUnmanagedAllocator allocator)
        {
            // may throw due to insufficient memory
            NotNulls = new BitVector(source.NotNulls, allocator);
        }

        public abstract Type ElementType { get; }
        public abstract DbType DbType { get; }

        public virtual bool TryEnsureCapacity(int newCapacity, int timeout = 0)
        {
            return NotNulls.TryEnsureCapacity((ulong)newCapacity, timeout);
        }

        /// <summary>
        /// Do not remove. Used implicitly from runtime code generator.
        /// </summary>
        public virtual void EnsureCapacity(int newCapacity)
        {
            if (!TryEnsureCapacity(newCapacity, Timeout.Infinite))
            {
                throw new Exception("Failed");
            }
        }

        public bool IsLoadingInProgress
        {
            get { return m_dataLoaderTasks != null; }
        }

        public bool AttachLoaders(Task[] loaders)
        {
            return null == Interlocked.CompareExchange(ref m_dataLoaderTasks, loaders, null);
        }

        public void WaitLoadingCompleted()
        {
            var loaders = m_dataLoaderTasks;
            if (loaders != null)
            {
                Task.WaitAll(loaders);
                Interlocked.CompareExchange(ref m_dataLoaderTasks, null, loaders);
            }
        }

        /// <summary>
        /// Disassembled method from BinaryReader.
        /// </summary>
        
        public static int Read7BitEncodedInt(BinaryReader reader)
        {
            int num1 = 0;
            int num2 = 0;
            while (num2 != 35)
            {
                byte num3 = reader.ReadByte();
                num1 |= ((int)num3 & (int)sbyte.MaxValue) << num2;
                num2 += 7;
                if (((int)num3 & 128) == 0)
                    return num1;
            }
            throw new FormatException("Format_Bad7BitInt32");
        }

        /// <summary>
        /// Disassembled method from BinaryReader.
        /// </summary>
        
        public static void Write7BitEncodedInt(BinaryWriter writer, int value)
        {
            uint num = (uint)value;
            while (num >= 128U)
            {
                writer.Write((byte)(num | 128U));
                num >>= 7;
            }
            writer.Write((byte)num);
        }

        
        public static SizableArrayOfByte ReadByteArray(BinaryReader reader)
        {
            var result = new SizableArrayOfByte();
            result.Length = Read7BitEncodedInt(reader);
            if (result.Length > 0)
            {
                result.Data = reader.ReadBytes(result.Length);
            }

            return result;
        }

        
        public static Guid ReadGuid(BinaryReader reader)
        {
            var buf = new DriverRowData.ValueHolder16Bytes {Lo = reader.ReadInt64(), Hi = reader.ReadInt64()};
            return buf.AsGuid;
        }

        /// <summary>
        /// Action to copy a value from driver row data into this column data.
        /// First argument stands for document index, second is reference to driver row, third is the index of column in the typed array.
        /// </summary>
        /// <remarks>NOTE: this method assumes that source value is NOT NULL. This should be verified by caller.</remarks>
        /// <seealso cref="DriverRowData.FieldArrayIndexes"/>
        public Action<int, DriverRowData, int> AssignFromDriverRow { get; protected set; }

        /// <summary>
        /// Action to copy a value from column data into driver row data.
        /// First argument stands for document index, second is reference to driver row, third is the index of column in the typed array.
        /// </summary>
        /// <remarks>NOTE: this method assumes that source value is NOT NULL. This should be verified by caller.</remarks>
        /// <seealso cref="DriverRowData.FieldArrayIndexes"/>
        public Action<int, DriverRowData, int> AssignToDriverRow { get; protected set; }

        /// <summary>
        /// Action to write actual data values to a binary stream.
        /// </summary>
        public Action<BinaryWriter, int> WriteData { get; protected set; }

        /// <summary>
        /// Action to read actual data values from a binary stream.
        /// </summary>
        public Action<BinaryReader, int> ReadData { get; protected set; }

        public void Dispose()
        {
            Dispose(true);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!m_disposed)
            {
                m_disposed = true;

                if (disposing)
                {
                    GC.SuppressFinalize(this);
                }

                if (NotNulls != null)
                {
                    NotNulls.Dispose();
                }
            }
        }

        ~ColumnDataBase()
        {
            Dispose(false);
        }
    }
}