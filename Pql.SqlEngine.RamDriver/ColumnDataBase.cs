using System.Data;

using Pql.ExpressionEngine.Interfaces;
using Pql.SqlEngine.Interfaces.Internal;
using Pql.UnmanagedLib;

namespace Pql.SqlEngine.DataContainer.RamDriver
{
    internal abstract class AColumnDataBase : IDisposable
    {
        private volatile Task[] _dataLoaderTasks;

        public BitVector NotNulls;
        private bool _disposed;

        protected AColumnDataBase(IUnmanagedAllocator allocator)
        {
            NotNulls = new BitVector(allocator);
        }

        protected AColumnDataBase(AColumnDataBase source, IUnmanagedAllocator allocator)
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

        public bool IsLoadingInProgress => _dataLoaderTasks != null;

        public bool AttachLoaders(Task[] loaders)
        {
            return null == Interlocked.CompareExchange(ref _dataLoaderTasks, loaders, null);
        }

        public void WaitLoadingCompleted()
        {
            var loaders = _dataLoaderTasks;
            if (loaders != null)
            {
                Task.WaitAll(loaders);
                Interlocked.CompareExchange(ref _dataLoaderTasks, null, loaders);
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
                num1 |= (num3 & sbyte.MaxValue) << num2;
                num2 += 7;
                if ((num3 & 128) == 0)
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
            if (!_disposed)
            {
                _disposed = true;

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

        ~AColumnDataBase()
        {
            Dispose(false);
        }
    }
}