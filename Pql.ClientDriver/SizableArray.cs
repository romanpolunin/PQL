using System.Diagnostics;

namespace Pql.ClientDriver
{
    /// <summary>
    /// A List-like array of arbitrary objects or value types.
    /// Directly exposes array of objects, to allow performance optimizations.
    /// </summary>
    /// <typeparam name="T">Type of the object to hold</typeparam>
    public sealed class SizableArray<T>
    {
        /// <summary>
        /// Array of elements. May contain more elements than actually used.
        /// </summary>
        /// <see cref="Length"/>
        public T[]? Data;

        /// <summary>
        /// Number of valid elements inside <see cref="Data"/>.
        /// </summary>
        public int Length;

        /// <summary>
        /// Capacity of the <see cref="Data"/> array or zero if it is null.
        /// </summary>
        public int Capacity
        {
            get
            {
                AssertState();

                return Data == null ? 0 : Data.Length;
            }
        }

        /// <summary>
        /// Ctr.
        /// </summary>
        public SizableArray()
        {

        }

        /// <summary>
        /// Ctr.
        /// </summary>
        /// <param name="capacity">Initial capacity</param>
        public SizableArray(int capacity)
        {
            EnsureCapacity(capacity);
        }

        /// <summary>
        /// Ctr.
        /// </summary>
        /// <param name="data">The buffer to take as initial storage</param>
        public SizableArray(T[]? data)
        {
            Data = data;
            Length = data == null ? 0 : data.Length;
        }

        /// <summary>
        /// Reallocates <see cref="Data"/> if needed to accomodate <paramref name="capacity"/> number of elements.
        /// </summary>
        public void EnsureCapacity(int capacity)
        {
            AssertState();

            if (capacity < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(capacity), "Capacity cannot be negative");
            }

            if (capacity > Capacity)
            {
                Array.Resize(ref Data, capacity);
            }
        }

        /// <summary>
        /// Reallocates <see cref="Data"/> if needed to accomodate <paramref name="length"/> number of elements.
        /// Updates value of <see cref="Length"/> too.
        /// </summary>        
        public void SetLength(int length)
        {
            EnsureCapacity(length);
            Length = length;
        }

        /// <summary>
        /// Verifies consistency.
        /// </summary>
        [Conditional("DEBUG")]
        private void AssertState() => Debug.Assert((Data == null && Length == 0) || (Data != null && Data.Length >= Length));
    }
}