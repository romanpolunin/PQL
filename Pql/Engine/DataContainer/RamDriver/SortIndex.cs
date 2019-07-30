using System;
using System.Collections.Generic;
using Pql.Engine.Interfaces.Internal;
using Pql.ExpressionEngine.Interfaces;
using Pql.UnmanagedLib;

namespace Pql.Engine.DataContainer.RamDriver
{
    internal class SortIndex
    {
        private int[] m_orderData;
        private int m_validDocCount;

        public int ValidDocCount
        {
            get { return m_validDocCount; }
            private set { m_validDocCount = value; }
        }

        public int[] OrderData
        {
            get { return m_orderData; }
            private set { m_orderData = value; }
        }

        public SortIndex()
        {
            OrderData = new int[0];
        }

        public void Update<T>(ColumnData<T> columnStore, BitVector validDocumentsBitmap, int count)
        {
            if (columnStore == null)
            {
                throw new ArgumentNullException("columnStore");
            }

            // construct proper comparer
            ItemComparer<T> comparer;
            var type = typeof(T);
            if (type.IsValueType)
            {
                comparer = new ItemComparer<T>(columnStore.NotNulls, columnStore.DataArray, Comparer<T>.Default);
            }
            else if (ReferenceEquals(type, typeof(string)))
            {
                comparer = new ItemComparer<T>(columnStore.NotNulls, columnStore.DataArray, (IComparer<T>)StringComparer.OrdinalIgnoreCase);
            }
            else if (ReferenceEquals(type, typeof(SizableArrayOfByte)))
            {
                comparer = new ItemComparer<T>(columnStore.NotNulls, columnStore.DataArray, (IComparer<T>)SizableArrayOfByte.DefaultComparer.Instance);
            }
            else
            {
                throw new Exception("Sort indexes are not supported for this type: " + type.FullName);
            }

            // reinitialize index with sequential documentIDs (initial order does not matter)
            // only use those document indexes that are not marked as deleted
            ArrayUtils.EnsureCapacity(ref m_orderData, count);
            m_validDocCount = 0;
            for (var i = 0; i < m_orderData.Length; i++)
            {
                if (validDocumentsBitmap.SafeGet(i))
                {
                    OrderData[m_validDocCount] = i;
                    m_validDocCount++;
                }
            }
            
            // now reorder those integers based on data values they point to
            Array.Sort(OrderData, 0, m_validDocCount, comparer);

            IsValid = true;
        }

        public class ItemComparer<TUnderlyingValue> : IComparer<int>
        {
            private readonly BitVector m_notNulls;
            private readonly ExpandableArray<TUnderlyingValue> m_data;
            private readonly IComparer<TUnderlyingValue> m_valueComparer;

            public ItemComparer(BitVector notNulls, ExpandableArray<TUnderlyingValue> data, IComparer<TUnderlyingValue> valueComparer)
            {
                m_notNulls = notNulls ?? throw new ArgumentNullException("notNulls");
                m_data = data ?? throw new ArgumentNullException("data");
                m_valueComparer = valueComparer ?? throw new ArgumentNullException("valueComparer");
            }

            public int Compare(int x, int y)
            {
                var notnullX = m_notNulls.SafeGet(x);
                var notnullY = m_notNulls.SafeGet(y);

                if (notnullX)
                {
                    if (notnullY)
                    {
                        return m_valueComparer.Compare(m_data[x], m_data[y]);
                    }

                    return 1;
                }

                if (notnullY)
                {
                    return -1;
                }

                return 0;
            }
        }

        public void Invalidate()
        {
            IsValid = false;
        }

        public bool IsValid { get; private set; }
    }
}