using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace Pql.ClientDriver
{
    /// <summary>
    /// Implements a collection of command parameters.
    /// </summary>
    public class PqlDataCommandParameterCollection : DbParameterCollection, IDataParameterCollection
    {
        private readonly object _thisLock;
        private readonly List<PqlDataCommandParameter> _parameters;

        /// <summary>
        /// Ctr.
        /// </summary>
        public PqlDataCommandParameterCollection()
        {
            _thisLock = new object();
            _parameters = new List<PqlDataCommandParameter>();
        }

        /// <summary>
        /// Internal direct access to typed parameters information.
        /// </summary>
        internal IReadOnlyList<PqlDataCommandParameter> ParametersData => _parameters;

        /// <summary>
        /// Returns an enumerator that iterates through a collection.
        /// </summary>
        /// <returns>
        /// An <see cref="T:System.Collections.IEnumerator"/> object that can be used to iterate through the collection.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override IEnumerator GetEnumerator() => _parameters.GetEnumerator();

        /// <summary>
        /// Returns the <see cref="T:System.Data.Common.DbParameter"/> object at the specified index in the collection.
        /// </summary>
        /// <returns>
        /// The <see cref="T:System.Data.Common.DbParameter"/> object at the specified index in the collection.
        /// </returns>
        /// <param name="index">The index of the <see cref="T:System.Data.Common.DbParameter"/> in the collection.</param>
        protected override DbParameter GetParameter(int index) => _parameters[index];

        /// <summary>
        /// Returns <see cref="T:System.Data.Common.DbParameter"/> the object with the specified name.
        /// </summary>
        /// <returns>
        /// The <see cref="T:System.Data.Common.DbParameter"/> the object with the specified name.
        /// </returns>
        /// <param name="parameterName">The name of the <see cref="T:System.Data.Common.DbParameter"/> in the collection.</param>
        protected override DbParameter GetParameter(string parameterName) => this[parameterName];

        /// <summary>
        /// Copies the elements of the <see cref="T:System.Collections.ICollection"/> to an <see cref="T:System.Array"/>, 
        /// starting at a particular <see cref="T:System.Array"/> index.
        /// </summary>
        /// <param name="array">The one-dimensional <see cref="T:System.Array"/> that is the destination of the elements copied from 
        /// <see cref="T:System.Collections.ICollection"/>. The <see cref="T:System.Array"/> must have zero-based indexing. </param>
        /// <param name="index">The zero-based index in <paramref name="array"/> at which copying begins. </param>
        /// <exception cref="T:System.ArgumentNullException"><paramref name="array"/> is null. </exception>
        /// <exception cref="T:System.ArgumentOutOfRangeException"><paramref name="index"/> is less than zero. </exception>
        /// <exception cref="T:System.ArgumentException"><paramref name="array"/> is multidimensional.-or- The number of elements 
        /// in the source <see cref="T:System.Collections.ICollection"/> is greater than the available space from <paramref name="index"/> 
        /// to the end of the destination <paramref name="array"/>.-or-The type of the source <see cref="T:System.Collections.ICollection"/> 
        /// cannot be cast automatically to the type of the destination <paramref name="array"/>.</exception>
        /// <filterpriority>2</filterpriority>
        public override void CopyTo(Array array, int index)
        {
            if (array == null)
            {
                throw new ArgumentNullException(nameof(array));
            }

            if (index < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(index), index, "Index may not be negative");
            }

            if (array.Rank != 1 || array.Length < _parameters.Count + index)
            {
                throw new ArgumentException("Invalid or too small array");
            }

            for (var i = 0; i < _parameters.Count; i++)
            {
                array.SetValue(_parameters[i], i + index);
            }
        }

        /// <summary>
        /// Sets the <see cref="T:System.Data.Common.DbParameter"/> object with the specified name to a new value.
        /// </summary>
        /// <param name="parameterName">The name of the <see cref="T:System.Data.Common.DbParameter"/> object in the collection.</param>
        /// <param name="value">The new <see cref="T:System.Data.Common.DbParameter"/> value.</param>
        protected override void SetParameter(string parameterName, DbParameter value) => this[parameterName] = value;

        /// <summary>
        /// Gets the number of elements contained in the <see cref="T:System.Collections.ICollection"/>.
        /// </summary>
        /// <returns>
        /// The number of elements contained in the <see cref="T:System.Collections.ICollection"/>.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override int Count => _parameters.Count;

        /// <summary>
        /// Gets an object that can be used to synchronize access to the <see cref="T:System.Collections.ICollection"/>.
        /// </summary>
        /// <returns>
        /// An object that can be used to synchronize access to the <see cref="T:System.Collections.ICollection"/>.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override object SyncRoot => _thisLock;

        /// <summary>
        /// Gets a value indicating whether access to the <see cref="T:System.Collections.ICollection"/> is synchronized (thread safe).
        /// </summary>
        /// <returns>
        /// true if access to the <see cref="T:System.Collections.ICollection"/> is synchronized (thread safe); otherwise, false.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override bool IsSynchronized => false;

        /// <summary>
        /// Adds an item to the <see cref="T:System.Collections.IList"/>.
        /// </summary>
        /// <returns>
        /// The position into which the new element was inserted, or -1 to indicate that the item was not inserted into the collection,
        /// </returns>
        /// <param name="value">The object to add to the <see cref="T:System.Collections.IList"/>. </param>
        /// <exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.IList"/> is read-only.
        /// -or- The <see cref="T:System.Collections.IList"/> has a fixed size. </exception>
        /// <filterpriority>2</filterpriority>
        public override int Add(object? value)
        {
            var typed = EnsureImplementationType(value);
            _parameters.Add(typed);
            return _parameters.Count - 1;
        }

        /// <summary>
        /// Adds an array of items with the specified values to the <see cref="T:System.Data.Common.DbParameterCollection"/>.
        /// </summary>
        /// <param name="values">An array of values of type <see cref="T:System.Data.Common.DbParameter"/> to add to the collection.</param>
        /// <filterpriority>2</filterpriority>
        public override void AddRange(Array values)
        {
            if (values != null)
            {
                foreach (var param in values)
                {
                    Add(param);
                }
            }
        }

        /// <summary>
        /// Determines whether the <see cref="T:System.Collections.IList"/> contains a specific value.
        /// </summary>
        /// <returns>
        /// true if the <see cref="T:System.Object"/> is found in the <see cref="T:System.Collections.IList"/>; otherwise, false.
        /// </returns>
        /// <param name="value">The object to locate in the <see cref="T:System.Collections.IList"/>. </param><filterpriority>2</filterpriority>
        public override bool Contains(object? value) => _parameters.Contains(EnsureImplementationType(value));

        /// <summary>
        /// Removes all items from the <see cref="T:System.Collections.IList"/>.
        /// </summary>
        /// <exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.IList"/> is read-only. </exception><filterpriority>2</filterpriority>
        public override void Clear() => _parameters.Clear();

        /// <summary>
        /// Determines the index of a specific item in the <see cref="T:System.Collections.IList"/>.
        /// </summary>
        /// <returns>
        /// The index of <paramref name="value"/> if found in the list; otherwise, -1.
        /// </returns>
        /// <param name="value">The object to locate in the <see cref="T:System.Collections.IList"/>. </param><filterpriority>2</filterpriority>
        public override int IndexOf(object? value) => _parameters.IndexOf(EnsureImplementationType(value));

        /// <summary>
        /// Inserts an item to the <see cref="T:System.Collections.IList"/> at the specified index.
        /// </summary>
        /// <param name="index">The zero-based index at which <paramref name="value"/> should be inserted. </param><param name="value">The object to insert into the <see cref="T:System.Collections.IList"/>. </param><exception cref="T:System.ArgumentOutOfRangeException"><paramref name="index"/> is not a valid index in the <see cref="T:System.Collections.IList"/>. </exception><exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.IList"/> is read-only.-or- The <see cref="T:System.Collections.IList"/> has a fixed size. </exception><exception cref="T:System.NullReferenceException"><paramref name="value"/> is null reference in the <see cref="T:System.Collections.IList"/>.</exception><filterpriority>2</filterpriority>
        public override void Insert(int index, object? value) => _parameters.Insert(index, EnsureImplementationType(value));

        /// <summary>
        /// Removes the first occurrence of a specific object from the <see cref="T:System.Collections.IList"/>.
        /// </summary>
        /// <param name="value">The object to remove from the <see cref="T:System.Collections.IList"/>. </param><exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.IList"/> is read-only.-or- The <see cref="T:System.Collections.IList"/> has a fixed size. </exception><filterpriority>2</filterpriority>
        public override void Remove(object? value) => _parameters.Remove(EnsureImplementationType(value));

        /// <summary>
        /// Removes the <see cref="T:System.Collections.IList"/> item at the specified index.
        /// </summary>
        /// <param name="index">The zero-based index of the item to remove. </param><exception cref="T:System.ArgumentOutOfRangeException"><paramref name="index"/> is not a valid index in the <see cref="T:System.Collections.IList"/>. </exception><exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.IList"/> is read-only.-or- The <see cref="T:System.Collections.IList"/> has a fixed size. </exception><filterpriority>2</filterpriority>
        public override void RemoveAt(int index) => _parameters.RemoveAt(index);

        /// <summary>
        /// Gets or sets the element at the specified index.
        /// </summary>
        /// <returns>
        /// The element at the specified index.
        /// </returns>
        /// <param name="index">The zero-based index of the element to get or set. </param><exception cref="T:System.ArgumentOutOfRangeException"><paramref name="index"/> is not a valid index in the <see cref="T:System.Collections.IList"/>. </exception><exception cref="T:System.NotSupportedException">The property is set and the <see cref="T:System.Collections.IList"/> is read-only. </exception><filterpriority>2</filterpriority>
        object IList.this[int index]
        {
            get => _parameters[index];
            set => _parameters[index] = EnsureImplementationType(value);
        }

        /// <summary>
        /// Gets a value indicating whether the <see cref="T:System.Collections.IList"/> is read-only.
        /// </summary>
        /// <returns>
        /// true if the <see cref="T:System.Collections.IList"/> is read-only; otherwise, false.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override bool IsReadOnly => false;

        /// <summary>
        /// Gets a value indicating whether the <see cref="T:System.Collections.IList"/> has a fixed size.
        /// </summary>
        /// <returns>
        /// true if the <see cref="T:System.Collections.IList"/> has a fixed size; otherwise, false.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override bool IsFixedSize => false;

        /// <summary>
        /// Gets a value indicating whether a parameter in the collection has the specified name.
        /// </summary>
        /// <returns>
        /// true if the collection contains the parameter; otherwise, false.
        /// </returns>
        /// <param name="parameterName">The name of the parameter. </param><filterpriority>2</filterpriority>
        public override bool Contains(string parameterName)
        {
            if (string.IsNullOrEmpty(parameterName))
            {
                throw new ArgumentNullException(nameof(parameterName));
            }

            foreach (var param in _parameters)
            {
                if (0 == string.Compare(param.ParameterName, parameterName, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Gets the location of the <see cref="T:System.Data.IDataParameter"/> within the collection.
        /// </summary>
        /// <returns>
        /// The zero-based location of the <see cref="T:System.Data.IDataParameter"/> within the collection.
        /// </returns>
        /// <param name="parameterName">The name of the parameter. </param><filterpriority>2</filterpriority>
        public override int IndexOf(string parameterName)
        {
            if (string.IsNullOrEmpty(parameterName))
            {
                throw new ArgumentNullException(nameof(parameterName));
            }

            for (var index = 0; index < _parameters.Count; index++)
            {
                var param = _parameters[index];
                if (0 == string.Compare(param.ParameterName, parameterName, StringComparison.OrdinalIgnoreCase))
                {
                    return index;
                }
            }

            return -1;
        }

        /// <summary>
        /// Removes the <see cref="T:System.Data.IDataParameter"/> from the collection.
        /// </summary>
        /// <param name="parameterName">The name of the parameter. </param><filterpriority>2</filterpriority>
        public override void RemoveAt(string parameterName)
        {
            var index = IndexOf(parameterName);
            if (index >= 0)
            {
                RemoveAt(index);
            }
        }

        /// <summary>
        /// Sets the <see cref="T:System.Data.Common.DbParameter"/> object at the specified index to a new value. 
        /// </summary>
        /// <param name="index">The index where the <see cref="T:System.Data.Common.DbParameter"/> object is located.</param>
        /// <param name="value">The new <see cref="T:System.Data.Common.DbParameter"/> value.</param>
        protected override void SetParameter(int index, DbParameter value) => throw new NotImplementedException();

        /// <summary>
        /// Gets or sets the parameter at the specified index.
        /// </summary>
        /// <returns>
        /// An <see cref="T:System.Object"/> at the specified index.
        /// </returns>
        /// <param name="parameterName">The name of the parameter to retrieve. </param><filterpriority>2</filterpriority>
        object IDataParameterCollection.this[string parameterName]
        {
            get
            {
                var index = IndexOf(parameterName);
                return index < 0 
                    ? throw new ArgumentException("Parameter with this name does not exist") 
                    : _parameters[index];
            }
            set
            {
                var typed = EnsureImplementationType(value);
                var index = IndexOf(parameterName);
                if (index < 0)
                {
                    _parameters.Add(typed);
                }
                else
                {
                    _parameters[index] = typed;
                }
            }
        }

        /// <summary>
        /// Validates consistency of configuration values assigned to parameters.
        /// </summary>
        public void Validate()
        {
            foreach (var param in _parameters)
            {
                param.Validate();
            }
        }

        private static PqlDataCommandParameter EnsureImplementationType(object? value)
        {
            return value == null
                ? throw new ArgumentNullException(nameof(value))
                : value is PqlDataCommandParameter typed
                ? typed
                : throw new ArgumentException("Invalid parameter instance type: " + value.GetType().FullName);
        }
    }
}