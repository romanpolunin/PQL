#pragma once

#include "tbb/critical_section.h"
#include "MemoryPoolTypes.h"

namespace Pql {
	namespace UnmanagedLib {

		template <typename T> class ExpandableArrayImpl
		{
		public:
			typedef T value_type;
			typedef value_type volatile* volatile* containerref_t;

		private:
			memorypoolallocator_t* m_pAllocator;
			size_t m_elementsPerBlock;
			size_t m_blocksGrowth;
			tbb::critical_section m_thisLock;

			containerref_t volatile m_blockList;
			size_t volatile m_blockCapacity;
			size_t volatile m_blockCount;

		public:
			ExpandableArrayImpl(memorypoolallocator_t* pAllocator, size_t elementsPerBlock, size_t blocksGrowth)
				: m_pAllocator(pAllocator), m_elementsPerBlock(elementsPerBlock), m_blocksGrowth(blocksGrowth)
			{
			}

			~ExpandableArrayImpl(void)
			{
				if (m_pAllocator)
				{
					if (m_blockList)
					{
						for (auto i = 0; i < m_blockCapacity; i++)
						{
							if (m_blockList[i])
							{
								m_pAllocator->deallocate((void*)m_blockList[i]);
							}
						}

						m_pAllocator->deallocate((void*)m_blockList);
						m_blockCapacity = 0;
						m_blockList = nullptr;
					}

					m_pAllocator = nullptr;
				}
			}

			inline bool try_ensure_capacity(size_t newCapacity, size_t timeout, interior_ptr<containerref_t> ref, interior_ptr<size_t> cap)
			{
				if (capacity() >= newCapacity)
				{
					return true;
				}

				tbb::critical_section::scoped_lock lock(m_thisLock);

				if (capacity() >= newCapacity)
				{
					return true;
				}

				// adjust new capacity value to desired granularity - may be larger than what's needed for this request
				auto requestedListCapacity = 1 + newCapacity / m_elementsPerBlock;
				auto newListCapacity = (1 + newCapacity / (m_elementsPerBlock*m_blocksGrowth)) * m_blocksGrowth;

				// make sure top-level list has enough capacity, with respect to our granularity setting
				if (m_blockCapacity < newListCapacity)
				{
					value_type** pNewList;
					try
					{
						pNewList = (value_type**)m_pAllocator->allocate(newListCapacity * sizeof(value_type*));
					}
					catch (System::InsufficientMemoryException^)
					{
						return false;
					}

					size_t ix;
					for (ix = 0; ix < m_blockCapacity; ix++)
					{
						pNewList[ix] = const_cast<value_type*>(m_blockList[ix]);
					}

					// do not deallocate current list immediately, some threads might be accessing it right now
					auto prevList = m_blockList;

					m_blockList = const_cast<containerref_t>(pNewList);

					if (prevList)
					{
						m_pAllocator->schedule_for_collection((void*)prevList);
					}

					tbb::full_fence;

					*ref = m_blockList;
					m_blockCapacity = newListCapacity;
				}

				// now allocate missing blocks up to requested number
				for (auto ix = m_blockCount; ix < requestedListCapacity; ix++)
				{
					try
					{
						(const_cast<value_type**>(m_blockList))[ix] = (value_type*)m_pAllocator->allocate(m_elementsPerBlock * sizeof(value_type));
					}
					catch (System::InsufficientMemoryException^)
					{
						m_blockCount = ix;
						*cap = capacity();
						return false;
					}
				}

				tbb::full_fence;

				m_blockCount = requestedListCapacity;
				*cap = capacity();

				return true;
			}

			inline size_t capacity() const { return m_blockCount * m_elementsPerBlock; }

			inline value_type get(size_t index) const
			{
				return *reference(index);
			}

			inline void set(size_t index, value_type newValue)
			{
				m_blockList[index / m_elementsPerBlock][index % m_elementsPerBlock] = newValue;
			}

			inline value_type volatile* volatile reference(size_t index) const
			{
				return &m_blockList[index / m_elementsPerBlock][index % m_elementsPerBlock];
			}

			inline containerref_t head() const
			{
				return m_blockList;
			}
		};
	}
}