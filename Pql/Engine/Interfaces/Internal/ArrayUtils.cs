using System;
using System.Runtime.CompilerServices;

namespace Pql.Engine.Interfaces.Internal
{
    public static class ArrayUtils
    {
        public static void RemoveRange<T>(T[] data, int from, int count, int elementSize)
        {
            if (from < 0)
            {
                throw new ArgumentOutOfRangeException("from", from, "From cannot be negative");
            }

            if (count < 0)
            {
                throw new ArgumentOutOfRangeException("count", count, "Count cannot be negative");
            }

            if (elementSize <= 0)
            {
                throw new ArgumentOutOfRangeException("elementSize", elementSize, "Element size must be positive");
            }

            var len = data.Length;
            if (from + count > len)
            {
                throw new ArgumentException(string.Format(
                    "Invalid combination of from ({0}) and count ({1}) values versus length ({2})", from, count, len));
            }

            Buffer.BlockCopy(data, from + count, data, from, count * elementSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void EnsureCapacity<T>(ref T[] data, int capacity)
        {
            if (capacity < 0)
            {
                throw new ArgumentOutOfRangeException("capacity", capacity, "Capacity cannot be negative");
            }

            if (data.Length < capacity)
            {
                Array.Resize(ref data, capacity);
            }
        }
    }
}