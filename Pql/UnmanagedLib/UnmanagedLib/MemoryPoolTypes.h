#pragma once

#include <cstdint>
#include "tbb/concurrent_unordered_map.h"
#include "tbb/memory_pool.h"
#include "tbb/concurrent_queue.h"
#include "tbb/tbb_allocator.h"
#include "Win32Imports.h"

namespace Pql {
	namespace UnmanagedLib {

		using namespace System::Runtime::CompilerServices;

		class MemoryPool;

		typedef MemoryPool memorypool_t;

		template <class T>
		class zero_memory_pool_allocator
		{
		public:
			typedef T value_type;
			typedef value_type* pointer;
			typedef const value_type* const_pointer;
			typedef value_type& reference;
			typedef const value_type& const_reference;
			typedef size_t size_type;
			typedef ptrdiff_t difference_type;

			template<typename U> struct rebind {
				typedef zero_memory_pool_allocator<U> other;
			};

		private:
			memorypool_t* m_pool;

		public:
			inline memorypool_t* get_pool() const { return m_pool; }

			zero_memory_pool_allocator(memorypool_t* pool) throw()
				: m_pool(pool)
			{}

			zero_memory_pool_allocator(const zero_memory_pool_allocator& src) throw()
				: m_pool(src.m_pool)
			{}

			template<typename U>
			zero_memory_pool_allocator(const zero_memory_pool_allocator<U>& src) throw()
				: m_pool(src.get_pool())
			{}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline void destroy(pointer p)
			{
				p->~value_type();
				//deallocate(p, sizeof(value_type));
			}

			//! Allocate space for n objects.
			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline pointer allocate(size_type n, const void* hint = 0) throw()
			{
				auto ptr = (pointer)m_pool->allocate(n * sizeof(value_type));
				if (!ptr)
				{
					throw gcnew System::InsufficientMemoryException("Failed to allocate " + n + " bytes");
				}

				memset(ptr, 0, n * sizeof(value_type));

				return ptr;
			}

			//! Allocate space for n objects.
			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline void deallocate(void* ptr, size_type size = 0)
			{
				m_pool->deallocate(ptr);
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline void schedule_for_collection(void* p) throw()
			{
				m_pool->schedule_for_collection(p);
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline void collect()
			{
				m_pool->collect();
			}
		};

		typedef zero_memory_pool_allocator<int8_t> memorypoolallocator_t;

#pragma unmanaged
		class MemoryPool
		{
			typedef tbb::memory_pool<tbb::scalable_allocator<uint8_t>> mypool_t;
			mypool_t m_pool;
			size_t m_maxBytes;
			tbb::concurrent_queue<void*, memorypoolallocator_t> m_garbage;

		public:

			MemoryPool(size_t maxBytes = 0) throw()
				: m_maxBytes(maxBytes), m_garbage(memorypoolallocator_t(this))
			{
			}

			~MemoryPool()
			{
			}

			inline void recycle()
			{
				m_garbage.clear();
				m_pool.recycle();
			}

			inline void* allocate(size_t n) throw()
			{
				return m_pool.malloc(n);
			}

			inline void deallocate(void* p)
			{
				m_pool.free(p);
			}

			inline void schedule_for_collection(void* p) throw()
			{
				//deallocate(p);
				m_garbage.push(p);
			}

			inline void collect()
			{
				void* p;
				while (m_garbage.try_pop(p))
				{
					deallocate(p);
				}
			}
		};

#pragma managed

		template <typename T> inline void DestroyDealloc(T* ptr, memorypoolallocator_t* allocator)
		{
			if (ptr)
			{
				ptr->~T();
				allocator->deallocate(ptr);
			}
		}
	}
}

