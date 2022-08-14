#pragma once

#include "MemoryPoolTypes.h"

namespace Pql
{
    namespace UnmanagedLib
    {
#pragma managed
        public interface class IUnmanagedAllocator : System::IDisposable
        {
            virtual System::IntPtr AllocIntPtr(size_t nBytes) = 0;
            virtual void* Alloc(size_t nBytes) = 0;
            virtual void Free(System::IntPtr p) = 0;
            virtual void Free(void* p) = 0;
            virtual void Recycle() = 0;
            virtual void ScheduleForCollection(void* p) = 0;
            virtual void DeallocateGarbage() = 0;
            virtual void* GetMemoryPool() = 0;
        };
    }
}