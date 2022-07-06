using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Pql.ClientDriver.Protocol
{
    /// <summary>
    /// Bitvector implementation.
    /// </summary>
    public static class BitVector
    {
        /// <summary>
        /// Number of bits per Int32.
        /// </summary>
        private const int BitsPerInt32 = sizeof(Int32) * 8;

        /// <summary>
        /// Bit accessor.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Invalid index</exception>        
        public static bool Get(int[] bits, int index) => (bits[index / BitsPerInt32] & (1 << (index % BitsPerInt32))) != 0;

        /// <summary>
        /// Bit accessor.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Invalid index</exception>        
        public static bool Get(IReadOnlyList<int> bits, int index) => (bits[index / BitsPerInt32] & (1 << (index % BitsPerInt32))) != 0;

        /// <summary>
        /// Bit setter.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Invalid index</exception>
        public static void Set(int[] bits, int index) => bits[index / BitsPerInt32] |= 1 << (index % BitsPerInt32);

        /// <summary>
        /// Bit setter.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Invalid index</exception>
        public static void Set(IList<int> bits, int index) => bits[index / BitsPerInt32] |= 1 << (index % BitsPerInt32);

        /// <summary>
        /// Bit setter.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Invalid index</exception>
        public static void Clear(int[] bits, int index) => bits[index / BitsPerInt32] &= ~(1 << (index % BitsPerInt32));
        /// <summary>
        /// Bit setter.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Invalid index</exception>
        public static void Clear(IList<int> bits, int index) => bits[index / BitsPerInt32] &= ~(1 << (index % BitsPerInt32));

        /// <summary>
        /// Bit accessor.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Invalid index</exception>        
        public static bool SafeGet(int[] bits, int index)
        {
            //var value = Interlocked.CompareExchange(ref bits[index / BitsPerInt32], 0, 0);
            var value = bits[index / BitsPerInt32];
            return (value & (1 << (index % BitsPerInt32))) != 0;
        }

        /// <summary>
        /// Bit accessor.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Invalid index</exception>        
        public static bool SafeGet(IReadOnlyList<int> bits, int index)
        {
            //var value = Interlocked.CompareExchange(ref bits[index / BitsPerInt32], 0, 0);
            var value = bits[index / BitsPerInt32];
            return (value & (1 << (index % BitsPerInt32))) != 0;
        }

        /// <summary>
        /// Bit setter.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Invalid index</exception>        
        public static void SafeSet(int[] bits, int index)
        {
            int oldValue;
            int value;
            do
            {
                //oldValue = Interlocked.CompareExchange(ref bits[index / BitsPerInt32], 0, 0);
                oldValue = bits[index / BitsPerInt32];
                value = oldValue | (1 << (index % BitsPerInt32));

                value = Interlocked.CompareExchange(ref bits[index / BitsPerInt32], value, oldValue);
            } while (value != oldValue);
        }

        /// <summary>
        /// Bit setter.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Invalid index</exception>
        public static void SafeClear(int[] bits, int index)
        {
            int oldValue;
            int value;
            do
            {
                //oldValue = Interlocked.CompareExchange(ref bits[index / BitsPerInt32], 0, 0);
                oldValue = bits[index / BitsPerInt32];
                value = oldValue & ~(1 << (index % BitsPerInt32));

                value = Interlocked.CompareExchange(ref bits[index / BitsPerInt32], value, oldValue);
            } while (value != oldValue);
        }

        /// <summary>
        /// Sets all bits to specified value.
        /// </summary>        
        public static void SetAll(int[] bits, bool value)
        {
            var num = value ? -1 : 0;
            if (bits != null)
            {
                for (var index = 0; index < bits.Length; ++index)
                {
                    bits[index] = num;
                }
            }
        }

        /// <summary>
        /// Computes length of Int32 array needed to store N bits.
        /// </summary>
        /// <param name="n">Number of bits</param>        
        public static Int32 GetArrayLength(Int32 n) => n <= 0 ? 0 : ((n - 1) / BitsPerInt32) + 1;

        /// <summary>
        /// Writes bitvector data to stream.
        /// </summary>
        /// <param name="bits">Bit data</param>
        /// <param name="count">Number of bool values stored in <paramref name="bits"/></param>
        /// <param name="writer">Stream writer</param>        
        public static void Write(int[] bits, int count, BinaryWriter writer)
        {
            if (writer == null)
            {
                throw new ArgumentNullException(nameof(writer));
            }

            if (count == 0)
            {
                return;
            }

            if (count < 0 || count > bits.Length * BitsPerInt32)
            {
                throw new ArgumentOutOfRangeException(nameof(count), count, "Count of bits does not match size of the bitvector data array");
            }

            var bitNumber = 0;
            while (bitNumber < count - BitsPerInt32)
            {
                writer.Write(bits[bitNumber / BitsPerInt32]);
                bitNumber += BitsPerInt32;
            }

            var tail = bits[bitNumber / BitsPerInt32];
            while (bitNumber < count)
            {
                writer.Write((byte)tail);
                tail >>= 8;
                bitNumber += 8;
            }
        }

        /// <summary>
        /// Reads bitvector data from stream.
        /// </summary>
        /// <param name="bits">Bit data</param>
        /// <param name="count">Number of elements in the bits data</param>
        /// <param name="reader">Stream reader</param>        
        public static void Read(int[] bits, int count, BinaryReader reader)
        {
            if (reader == null)
            {
                throw new ArgumentNullException(nameof(reader));
            }

            if (count == 0)
            {
                return;
            }

            if (count < 0 || count > bits.Length * BitsPerInt32)
            {
                throw new ArgumentOutOfRangeException(nameof(count), count, "Count of bits does not match size of the bitvector data array");
            }

            var bitNumber = 0;
            while (bitNumber < count - BitsPerInt32)
            {
                bits[bitNumber / BitsPerInt32] = reader.ReadInt32();
                bitNumber += BitsPerInt32;
            }

            int tail = 0;
            if (bitNumber < count)
            {
                tail = reader.ReadByte();
                bitNumber += 8;

                if (bitNumber < count)
                {
                    tail += reader.ReadByte() << 8;
                    bitNumber += 8;

                    if (bitNumber < count)
                    {
                        tail += reader.ReadByte() << 16;
                        bitNumber += 8;

                        if (bitNumber < count)
                        {
                            tail += reader.ReadByte() << 24;
                        }
                    }
                }
            }

            bits[bitNumber / BitsPerInt32] = tail;
        }

        /// <summary>
        /// Calculates the number of bytes needed to represent given number of bits.
        /// </summary>
        /// <param name="size">Number of bits</param>        
        public static int GetByteCount(int size) => 0 == size % 8 ? size / 8 : 1 + (size / 8);
    }
}