using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Pql.ExpressionEngine.Interfaces
{
    /// <summary>
    /// A data structure to hold byte arrays.
    /// </summary>
    public class SizableArrayOfByte
    {
        /// <summary>
        /// Actual bytes.
        /// </summary>
        public byte[]? Data;
        /// <summary>
        /// Number of bytes in <see cref="Data"/> which have meaningful data.
        /// </summary>
        public int Length;

        /// <summary>
        /// Pre-computed hash code. 
        /// Is invalidated by <see cref="SetLength"/> and <see cref="EnsureCapacity"/>.
        /// </summary>
        public int HashCode;

        /// <summary>
        /// Ctr.
        /// </summary>
        public SizableArrayOfByte()
        { }

        /// <summary>
        /// Ctr.
        /// Copies data from <paramref name="src"/>.
        /// </summary>
        public SizableArrayOfByte(byte[] src)
        {
            CopyFrom(src);
        }

        /// <summary>
        /// Ctr.
        /// Copies data from <paramref name="src"/>, interpreted as Base64 string.
        /// </summary>
        public SizableArrayOfByte(string src)
        {
            if (string.IsNullOrEmpty(src))
            {
                SetLength(0);
            }
            else
            {
                var data = Convert.FromBase64String(src);
                if (Data == null)
                {
                    Data = data;
                    Length = data.Length;
                }
                else
                {
                    // yes, we will discard this data array that was just allocated
                    // main reason is to keep our existing Data alive, because it is very likely to be in GEN2
                    CopyFrom(data);
                }
            }
        }

        /// <summary>
        /// Generates a base64 string out of this binary array.
        /// </summary>
        public static string? ToBase64String(SizableArrayOfByte array)
        {
            if (array == null || array.Length == 0)
            {
                return null;
            }

            return Convert.ToBase64String(array.Data, 0, array.Length, Base64FormattingOptions.None);
        }

        /// <summary>
        /// Ctr.
        /// Copies data from <paramref name="src"/>.
        /// </summary>
        public SizableArrayOfByte(SizableArrayOfByte src)
        {
            CopyFrom(src);
        }

        /// <summary>
        /// Copies data from <paramref name="src"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyFrom(SizableArrayOfByte src)
        {
            if (src == null)
            {
                SetLength(0);
            }
            else
            {
                SetLength(src.Length);
                if (Length > 0)
                {
                    Buffer.BlockCopy(src.Data, 0, Data, 0, Length);
                }
                HashCode = src.HashCode;
            }
        }

        /// <summary>
        /// Copies data from <paramref name="src"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyFrom(byte[] src)
        {
            if (src == null)
            {
                SetLength(0);
            }
            else
            {
                SetLength(src.Length);
                if (Length > 0)
                {
                    Buffer.BlockCopy(src, 0, Data, 0, Length);
                }
            }
        }

        /// <summary>
        /// Appends a byte at the end.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Append(byte item)
        {
            AssertState();

            var index = Length;
            EnsureCapacity(index + 1);
            Data[index] = item;
        }

        /// <summary>
        /// Removes specified range of bytes and shifts tail up.
        /// </summary>
        public void RemoveRange(int from, int count)
        {
            AssertState();

            if (from < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(from), from, "From cannot be negative");
            }

            if (count < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(count), count, "Count cannot be negative");
            }

            if (from + count > Length)
            {
                throw new ArgumentException(string.Format(
                    "Invalid combination of from ({0}) and count ({1}) values versus length ({2})", from, count, Length));
            }

            Buffer.BlockCopy(Data, from + count, Data, from, count);
            Length -= count;
        }

        /// <summary>
        /// Trims actual size of <see cref="Data"/> down to <see cref="Length"/> number of bytes.
        /// </summary>
        public void Trim()
        {
            AssertState();

            Array.Resize(ref Data, Length);
        }

        /// <summary>
        /// Ensures that <see cref="Data"/> can hold <paramref name="capacity"/> bytes.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void EnsureCapacity(int capacity)
        {
            AssertState();

#if DEBUG
            if (capacity < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(capacity), capacity, "Capacity cannot be negative");
            }
#endif

            // invalidate precomputed hashcode value
            HashCode = 0;

            if (Length < capacity && capacity > 0)
            {
                if (Data == null)
                {
                    Data = new byte[capacity];
                }
                else if (Data.Length < capacity)
                {
                    Array.Resize(ref Data, capacity);
                }
            }
        }

        /// <summary>
        /// Ensures capacity of <see cref="Data"/> and also assigns <see cref="Length"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetLength(int length)
        {
            EnsureCapacity(length);
            Length = length;
        }

        /// <summary>
        /// Verifies consistency.
        /// </summary>
        [Conditional("DEBUG")]
        protected void AssertState()
        {
            Debug.Assert((Data == null && Length == 0) || (Data != null && Data.Length >= Length));
        }

        /// <summary>
        /// Default comparer for byte arrays.
        /// </summary>
        public class DefaultComparer : IEqualityComparer<SizableArrayOfByte>, IComparer<SizableArrayOfByte>
        {
            static DefaultComparer()
            {
                Instance = new DefaultComparer();
            }

            /// <summary>
            /// Default instance of comparer.
            /// </summary>
            public static DefaultComparer Instance { get; private set; }

            /// <summary>
            /// Determines whether the specified objects are equal.
            /// </summary>
            /// <returns>
            /// true if the specified objects are equal; otherwise, false.
            /// </returns>
            /// <param name="x">The first object to compare.</param>
            /// <param name="y">The second object to compare.</param>
            public bool Equals(SizableArrayOfByte x, SizableArrayOfByte y)
            {
                if (ReferenceEquals(x, y))
                {
                    return true;
                }

                if (x == null || y == null)
                {
                    return false;
                }

                if (x.Length != y.Length)
                {
                    return false;
                }

                for (var i = 0; i < x.Length; i++)
                {
                    if (x.Data[i] != y.Data[i])
                    {
                        return false;
                    }
                }

                return true;
            }

            /// <summary>
            /// Returns a hash code for the specified object.
            /// </summary>
            /// <returns>
            /// A hash code for the specified object.
            /// </returns>
            /// <param name="obj">The <see cref="T:System.Object"/> for which a hash code is to be returned.</param>
            /// <exception cref="T:System.ArgumentNullException">The type of <paramref name="obj"/> is a reference type and <paramref name="obj"/> is null.</exception>
            public int GetHashCode(SizableArrayOfByte obj)
            {
                if (obj == null)
                {
                    return -1;
                }

                if (obj.HashCode != 0)
                {
                    return obj.HashCode;
                }

                var len = obj.Length;
                if (len > 0)
                {
                    var data = obj.Data;
                    unchecked
                    {
                        const int p = 16777619;
                        int hash = (int)2166136261;

                        foreach (var b in data)
                        {
                            hash = (hash ^ b) * p;
                        }

                        //hash += hash << 13;
                        //hash ^= hash >> 7;
                        //hash += hash << 3;
                        //hash ^= hash >> 17;
                        //hash += hash << 5;

                        obj.HashCode = hash;
                    }
                }
                else
                {
                    obj.HashCode = 0;
                }

                return obj.HashCode;
            }

            /// <summary>
            /// Compares two objects and returns a value indicating whether one is less than, equal to, or greater than the other.
            /// </summary>
            /// <returns>
            /// A signed integer that indicates the relative values of <paramref name="x"/> and <paramref name="y"/>, as shown in the following table.
            /// Value Meaning Less than zero<paramref name="x"/> is less than <paramref name="y"/>.Zero<paramref name="x"/> equals <paramref name="y"/>.
            /// Greater than zero<paramref name="x"/> is greater than <paramref name="y"/>.
            /// </returns>
            /// <param name="x">The first object to compare.</param>
            /// <param name="y">The second object to compare.</param>
            public int Compare(SizableArrayOfByte x, SizableArrayOfByte y)
            {
                if (ReferenceEquals(x, y))
                {
                    return 0;
                }

                if (x == null)
                {
                    return -1;
                }

                if (y == null)
                {
                    return 1;
                }

                for (var i = 0; i < x.Length && i < y.Length; i++)
                {
                    if (x.Data[i] < y.Data[i])
                    {
                        return -1;
                    }
                    if (x.Data[i] > y.Data[i])
                    {
                        return 1;
                    }
                }

                if (x.Length < y.Length)
                {
                    return -1;
                }

                if (x.Length > y.Length)
                {
                    return 1;
                }

                return 0;
            }
        }
    }
}