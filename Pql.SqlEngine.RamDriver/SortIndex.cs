using Pql.ExpressionEngine.Interfaces;
using Pql.SqlEngine.Interfaces;
using Pql.SqlEngine.Interfaces.Internal;

namespace Pql.SqlEngine.DataContainer.RamDriver
{
    internal class SortIndex
    {
        private int[] _orderData;
        private int _validDocCount;

        public int ValidDocCount
        {
            get => _validDocCount;
            private set => _validDocCount = value;
        }

        public int[] OrderData
        {
            get => _orderData;
            private set => _orderData = value;
        }

        public SortIndex()
        {
            OrderData = new int[0];
        }

        public void Update<T>(ColumnData<T> columnStore, BitVector validDocumentsBitmap, int count)
        {
            if (columnStore == null)
            {
                throw new ArgumentNullException(nameof(columnStore));
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
            ArrayUtils.EnsureCapacity(ref _orderData, count);
            _validDocCount = 0;
            for (var i = 0; i < _orderData.Length; i++)
            {
                if (validDocumentsBitmap.SafeGet(i))
                {
                    OrderData[_validDocCount] = i;
                    _validDocCount++;
                }
            }
            
            // now reorder those integers based on data values they point to
            Array.Sort(OrderData, 0, _validDocCount, comparer);

            IsValid = true;
        }

        public class ItemComparer<TUnderlyingValue> : IComparer<int>
        {
            private readonly BitVector _notNulls;
            private readonly ExpandableArray<TUnderlyingValue> _data;
            private readonly IComparer<TUnderlyingValue> _valueComparer;

            public ItemComparer(BitVector notNulls, ExpandableArray<TUnderlyingValue> data, IComparer<TUnderlyingValue> valueComparer)
            {
                _notNulls = notNulls ?? throw new ArgumentNullException(nameof(notNulls));
                _data = data ?? throw new ArgumentNullException(nameof(data));
                _valueComparer = valueComparer ?? throw new ArgumentNullException(nameof(valueComparer));
            }

            public int Compare(int x, int y)
            {
                var notnullX = _notNulls.SafeGet(x);
                var notnullY = _notNulls.SafeGet(y);

                if (notnullX)
                {
                    if (notnullY)
                    {
                        return _valueComparer.Compare(_data[x], _data[y]);
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