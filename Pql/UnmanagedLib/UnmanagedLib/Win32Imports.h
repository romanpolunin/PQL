#pragma once

#include <cstdint>

namespace Pql {
	namespace UnmanagedLib {

		extern "C" uint8_t __fastcall UnmanagedLib_InterlockedCompareExchange8(volatile uint8_t*, uint8_t, uint8_t);
		extern "C" uint32_t __fastcall UnmanagedLib_InterlockedCompareExchange32(volatile uint32_t*, uint32_t, uint32_t);
		extern "C" uint64_t __fastcall UnmanagedLib_InterlockedCompareExchange64(volatile uint64_t*, uint64_t, uint64_t);
		extern "C" void* __fastcall UnmanagedLib_InterlockedCompareExchangePointer(void*volatile*, void*, void*);

		[System::Runtime::InteropServices::DllImport("kernel32")]
		extern "C" uint32_t __stdcall HeapFree(void* hHeap, uint32_t flags, void* pMem);
		[System::Runtime::InteropServices::DllImport("kernel32")]
		extern "C" void* __stdcall HeapAlloc(void* hHeap, uint32_t flags, uint64_t nBytes);
		[System::Runtime::InteropServices::DllImport("kernel32")]
		extern "C" void* __stdcall HeapCreate(uint32_t flags, uint64_t initialBytes, uint64_t maxBytes);
		[System::Runtime::InteropServices::DllImport("kernel32")]
		extern "C" uint32_t  __stdcall HeapDestroy(void* hHeap);
	}
}