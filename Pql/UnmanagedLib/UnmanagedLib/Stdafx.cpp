// stdafx.cpp : source file that includes just the standard includes
// UnmanagedLib.pch will be the pre-compiled header
// stdafx.obj will contain the pre-compiled type information

#include "stdafx.h"

#include "ExpandableArrayOfKeys.h"
#include "MemoryPoolTypes.h"
#include "MemoryManagerException.h"
#include "FixedMemoryPool.h"
#include "DynamicMemoryPool.h"
#include "BitVector.h"
#include "MemoryViewStream.h"
#include "ConcurrentHashmapOfKeys.h"
//#include "ExpandableArrayOfValues.h"
#include "Win32imports.h"

#pragma unmanaged

namespace Pql {
	namespace UnmanagedLib
	{

		extern "C" uint8_t __fastcall UnmanagedLib_InterlockedCompareExchange8(volatile uint8_t* pTarget, uint8_t value, uint8_t comparand)
		{
			return _InterlockedCompareExchange8((volatile char*)pTarget, value, comparand);
		}

		extern "C" uint32_t __fastcall UnmanagedLib_InterlockedCompareExchange32(volatile uint32_t* pTarget, uint32_t value, uint32_t comparand)
		{
			return _InterlockedCompareExchange(pTarget, value, comparand);
		}

		extern "C" uint64_t __fastcall UnmanagedLib_InterlockedCompareExchange64(volatile uint64_t* pTarget, uint64_t value, uint64_t comparand)
		{
			return _InterlockedCompareExchange64((volatile int64_t*)pTarget, value, comparand);
		}

		extern "C" void* __fastcall UnmanagedLib_InterlockedCompareExchangePointer(void*volatile* pTarget, void* value, void* comparand)
		{
			return _InterlockedCompareExchangePointer(pTarget, value, comparand);
		}
	}
}