#pragma once

#include "MemoryPoolTypes.h"

namespace Pql {
	namespace UnmanagedLib {

		class DynamicMemoryPoolImpl
		{
			memorypool_t m_pool;
			memorypoolallocator_t m_allocator;

		public:

			DynamicMemoryPoolImpl() : m_allocator(&m_pool) {}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline void* DynamicMemoryPoolImpl::allocate(size_t nBytes)
			{
				return m_allocator.allocate(nBytes, nullptr);
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline void DynamicMemoryPoolImpl::free(void* p)
			{
				m_allocator.deallocate(p);
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline void DynamicMemoryPoolImpl::recycle()
			{
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