#pragma once

#include "MemoryManagerException.h"
#include "IUnmanagedAllocator.h"

namespace Pql 
{
	namespace UnmanagedLib 
	{
		public ref class DynamicMemoryPool : IUnmanagedAllocator
		{
			MemoryPool* _pool;

			void Cleanup(bool disposing)
			{
				if (disposing)
				{
					System::GC::SuppressFinalize(this);
				}

				if (_pool)
				{
					try { delete _pool; }
					finally { _pool = nullptr; }
				}
			}

			!DynamicMemoryPool()
			{
				Cleanup(false);
			}

		public:
			DynamicMemoryPool() : _pool(new MemoryPool())
			{
			}

			~DynamicMemoryPool()
			{
				Cleanup(true);
			}

			inline virtual System::IntPtr AllocIntPtr(size_t nBytes)
			{
				return System::IntPtr(Alloc(nBytes));
			}

			inline virtual void* Alloc(size_t nBytes)
			{
				return _pool->allocate(nBytes);
			}

			inline virtual void Free(System::IntPtr p)
			{
				_pool->deallocate(static_cast<void*>(p.ToPointer()));
			}

			inline virtual void Free(void* p)
			{
				_pool->deallocate(p);
			}

			inline virtual void Recycle()
			{
				_pool->recycle();
			}

			inline virtual void ScheduleForCollection(void* p)
			{
				_pool->schedule_for_collection(p);
			}

			inline virtual void DeallocateGarbage()
			{
				_pool->collect();
			}

			inline virtual void* GetMemoryPool()
			{
				return _pool;
			}
		};
	}
}