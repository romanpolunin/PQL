#pragma once

#include "MemoryPoolTypes.h"

namespace Pql {
	namespace UnmanagedLib {

		class FixedMemoryPoolImpl
		{
			memorypool_t m_pool;
			memorypoolallocator_t m_allocator;

		public:

			FixedMemoryPoolImpl(size_t nBytes) : m_pool(nBytes), m_allocator(&m_pool) {}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline void* FixedMemoryPoolImpl::allocate(size_t nBytes)
			{
				return m_allocator.allocate(nBytes, nullptr);
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline void FixedMemoryPoolImpl::free(void* p)
			{
				m_allocator.deallocate(p);
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline void FixedMemoryPoolImpl::recycle()
			{
				m_allocator = memorypoolallocator_t(&m_pool);
				m_pool.recycle();
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline memorypoolallocator_t* my_allocator() { return &m_allocator; }

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline void schedule_for_collection(void* p)
			{
				m_pool.schedule_for_collection(p);
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline void collect()
			{
				m_pool.collect();
			}
		};
	}
}