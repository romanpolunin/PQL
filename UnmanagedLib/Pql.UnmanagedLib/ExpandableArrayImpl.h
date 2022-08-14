#pragma once

#include <atomic>
#include "oneapi/tbb/mutex.h"
#include "MemoryPoolTypes.h"

namespace Pql
{
    namespace UnmanagedLib
    {
        template <typename T> class ExpandableArrayImpl
        {
        public:
            using value_type = T;
            using containerref_t = value_type volatile* volatile*;
            using my_allocator_t = zero_memory_pool_allocator<T>;

        private:
            MemoryPool* _pool;
            my_allocator_t _allocator;
            size_t _elementsPerBlock;
            size_t _blocksGrowth;
            oneapi::tbb::spin_mutex _thisLock;

            containerref_t volatile _blockList;
            size_t volatile _blockCapacity;
            size_t volatile _blockCount;

        public:
            ExpandableArrayImpl(MemoryPool* pool, size_t elementsPerBlock, size_t blocksGrowth)
                : _pool(pool), _allocator(pool->get_pool()), _elementsPerBlock(elementsPerBlock), _blocksGrowth(blocksGrowth)
            {
            }

            ~ExpandableArrayImpl(void)
            {
                if (_blockList)
                {
                    for (auto i = 0; i < _blockCapacity; i++)
                    {
                        if (_blockList[i])
                        {
                            _pool->deallocate((void*)_blockList[i]);
                        }
                    }

                    _pool->deallocate((void*)_blockList);
                    _blockCapacity = 0;
                    _blockList = nullptr;
                }
            }

            inline bool try_ensure_capacity(size_t newCapacity, size_t timeout, interior_ptr<containerref_t> ref, interior_ptr<size_t> cap)
            {
                if (capacity() >= newCapacity)
                {
                    return true;
                }

                tbb::spin_mutex::scoped_lock lock(_thisLock);

                if (capacity() >= newCapacity)
                {
                    return true;
                }

                // adjust new capacity value to desired granularity - may be larger than what's needed for this request
                auto requestedListCapacity = 1 + newCapacity / _elementsPerBlock;
                auto newListCapacity = (1 + newCapacity / (_elementsPerBlock * _blocksGrowth)) * _blocksGrowth;

                // make sure top-level list has enough capacity, with respect to our granularity setting
                if (_blockCapacity < newListCapacity)
                {
                    value_type** pNewList;
                    try
                    {
                        pNewList = (value_type**)_allocator.allocate(newListCapacity);
                    }
                    catch (System::InsufficientMemoryException^)
                    {
                        return false;
                    }

                    size_t ix;
                    for (ix = 0; ix < _blockCapacity; ix++)
                    {
                        pNewList[ix] = const_cast<value_type*>(_blockList[ix]);
                    }

                    // do not deallocate current list immediately, some threads might be accessing it right now
                    auto prevList = _blockList;

                    _blockList = const_cast<containerref_t>(pNewList);

                    if (prevList)
                    {
                        _pool->schedule_for_collection((void*)prevList);
                    }

                    std::atomic_thread_fence(std::memory_order::memory_order_seq_cst);

                    *ref = _blockList;
                    _blockCapacity = newListCapacity;
                }

                // now allocate missing blocks up to requested number
                for (auto ix = _blockCount; ix < requestedListCapacity; ix++)
                {
                    try
                    {
                        (const_cast<value_type**>(_blockList))[ix] = (value_type*)_allocator.allocate(_elementsPerBlock);
                    }
                    catch (System::InsufficientMemoryException^)
                    {
                        _blockCount = ix;
                        *cap = capacity();
                        return false;
                    }
                }

                std::atomic_thread_fence(std::memory_order::memory_order_seq_cst);

                _blockCount = requestedListCapacity;
                *cap = capacity();

                return true;
            }

            inline size_t capacity() const { return _blockCount * _elementsPerBlock; }

            inline value_type get(size_t index) const
            {
                return *reference(index);
            }

            inline void set(size_t index, value_type newValue)
            {
                _blockList[index / _elementsPerBlock][index % _elementsPerBlock] = newValue;
            }

            inline value_type volatile* volatile reference(size_t index) const
            {
                return &_blockList[index / _elementsPerBlock][index % _elementsPerBlock];
            }

            inline containerref_t head() const
            {
                return _blockList;
            }
        };
    }
}