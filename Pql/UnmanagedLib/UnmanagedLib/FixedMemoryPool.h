#pragma once

#include "FixedMemoryPoolImpl.h"
#include "MemoryManagerException.h"
#include "IUnmanagedAllocator.h"

namespace Pql {
	namespace UnmanagedLib {

		public ref class FixedMemoryPool : IUnmanagedAllocator
		{
			FixedMemoryPoolImpl* m_pool;

			void Cleanup(bool disposing)
			{
				if (disposing)
				{
					System::GC::SuppressFinalize(this);
				}

				if (m_pool)
				{
					try { delete m_pool; }
					finally { m_pool = nullptr; }
				}
			}

			!FixedMemoryPool()
			{
				Cleanup(false);
			}

		public:
			FixedMemoryPool(size_t nBytes)
			{
				m_pool = new FixedMemoryPoolImpl(nBytes);
			}

			~FixedMemoryPool()
			{
				Cleanup(true);
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline virtual System::IntPtr AllocIntPtr(size_t nBytes)
			{
				return System::IntPtr(Alloc(nBytes));
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline virtual void* Alloc(size_t nBytes)
			{
				return m_pool->allocate(nBytes);
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline virtual void Free(System::IntPtr p)
			{
				m_pool->free(static_cast<void*>(p.ToPointer()));
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline virtual void Free(void* p)
			{
				m_pool->free(p);
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline virtual void Recycle()
			{
				m_pool->recycle();
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline virtual memorypoolallocator_t* GetAllocator()
			{
				return m_pool->my_allocator();
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline virtual void ScheduleForCollection(void *p)
			{
				m_pool->schedule_for_collection(p);
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline virtual void DeallocateGarbage()
			{
				m_pool->collect();
			}
		};
	}
}