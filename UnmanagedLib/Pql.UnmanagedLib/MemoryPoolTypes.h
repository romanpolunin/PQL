#pragma once
#pragma unmanaged

#include <cstdint>
#include "oneapi/tbb/concurrent_unordered_map.h"
#include "oneapi/tbb/memory_pool.h"
#include "oneapi/tbb/concurrent_queue.h"
#include "oneapi/tbb/tbb_allocator.h"
#include "Win32Imports.h"

namespace Pql
{
    namespace UnmanagedLib
    {
        typedef tbb::scalable_allocator<uint8_t> tbb_allocator_t;
        typedef tbb::memory_pool<tbb_allocator_t> tbb_memory_pool_t;

        template<typename T>
        class zero_memory_pool_allocator
        {
        private:
            tbb_memory_pool_t* _pool;
        public:
            using value_type = T;
            using propagate_on_container_move_assignment = std::true_type;
            using is_always_equal = std::false_type;

            zero_memory_pool_allocator(tbb_memory_pool_t* pool) throw()
            {
                _pool = pool;//  throw gcnew System::ArgumentNullException("pool");
            }

            tbb_memory_pool_t* get_pool() const { return _pool; }

            template<typename U> zero_memory_pool_allocator(const zero_memory_pool_allocator<U>& other) noexcept
            {
                _pool = other.get_pool();
            }

            [[nodiscard]] T* allocate(std::size_t n)
            {
                auto bytes = n * sizeof(value_type);
                auto result = static_cast<T*>(_pool->malloc(bytes));
                if (result != nullptr)
                {
                    memset(result, 0, bytes);
                }
                return result;
            }

            void deallocate(T* p, std::size_t)
            {
                _pool->free(p);
            }
        };

        template<typename T, typename U>
        inline bool operator == (const zero_memory_pool_allocator<T>& x, const zero_memory_pool_allocator<U>& y) noexcept
        {
            return x._pool == y._pool;
        }

        template<typename T, typename U>
        inline bool operator != (const zero_memory_pool_allocator<T>& x, const zero_memory_pool_allocator<U>& y) noexcept
        {
            return x._pool != y._pool;
        }

        class MemoryPool
        {
        public:
            using my_allocator_t = zero_memory_pool_allocator<uint8_t>;

        private:
            tbb_memory_pool_t _pool;
            my_allocator_t _allocator;
            size_t _maxBytes;
            tbb::concurrent_queue<void*, my_allocator_t> _garbage;

        public:

            MemoryPool(size_t maxBytes = 0) throw()
                : _maxBytes(maxBytes), _pool(), _allocator(&_pool), _garbage(_allocator)
            {
            }

            ~MemoryPool()
            {
            }

            inline const my_allocator_t* get_allocator() const { return &_allocator; }
            inline tbb_memory_pool_t* get_pool() { return &_pool; }

            inline void recycle()
            {
                _garbage.clear();
                _pool.recycle();
            }

            inline void* allocate(size_t n) throw()
            {
                return _allocator.allocate(n);
            }

            inline void deallocate(void* p)
            {
                _pool.free(p);
            }

            inline void schedule_for_collection(void* p) throw()
            {
                //deallocate(p);
                _garbage.push(p);
            }

            inline void collect()
            {
                void* p;
                while (_garbage.try_pop(p))
                {
                    deallocate(p);
                }
            }
        };

#pragma managed

        template <typename T> inline void DestroyDealloc(T* ptr, zero_memory_pool_allocator<T>* allocator)
        {
            if (ptr)
            {
                ptr->~T();
                allocator->deallocate(ptr);
            }
        }
    }
}

