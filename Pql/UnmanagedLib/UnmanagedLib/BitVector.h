#pragma once

#include <memory>
#include "IUnmanagedAllocator.h"
#include "ExpandableArrayImpl.h"

namespace Pql {
	namespace UnmanagedLib {

		using namespace System::Runtime::CompilerServices;

		public ref class BitVector
		{
			typedef ExpandableArrayImpl<uint8_t> dataarray_t;

#define ITEMS_PER_BLOCK 65536
#define BITS_PER_ITEM 8
#define BITS_PER_BLOCK (ITEMS_PER_BLOCK * BITS_PER_ITEM)
#define BLOCKS_GROWTH 64
#define CAS UnmanagedLib_InterlockedCompareExchange8

			IUnmanagedAllocator^ m_allocator;
			dataarray_t* m_pArray;
			dataarray_t::containerref_t volatile m_pData;
			size_t volatile m_itemCapacity;

			void Cleanup(bool disposing)
			{
				if (disposing)
				{
					System::GC::SuppressFinalize(this);
				}

				// simply discard the reference
				// rely upon pool management to clean up the garbage
				m_pArray = nullptr;
				m_pData = nullptr;
				m_itemCapacity = 0;
			}

			!BitVector()
			{
				Cleanup(false);
			}

			void Initialize(BitVector^ src, IUnmanagedAllocator^ allocator)
			{
				if (!allocator)
				{
					throw gcnew System::ArgumentNullException("allocator");
				}

				m_allocator = allocator;

				auto pobj = (dataarray_t*)m_allocator->Alloc(sizeof(dataarray_t));

				m_pArray = new (pobj)dataarray_t(m_allocator->GetAllocator(), ITEMS_PER_BLOCK, BLOCKS_GROWTH);

				if (src)
				{
					auto cap = src->Capacity;
					EnsureCapacity(cap);

					for (size_t ix = 0; ix < cap; ix++)
					{
						if (src->Get(ix)) Set(ix);
					}
				}
			}

		public:

			BitVector(IUnmanagedAllocator^ allocator)
			{
				Initialize(nullptr, allocator);
			}

			BitVector(BitVector^ src, IUnmanagedAllocator^ allocator)
			{
				Initialize(src, allocator);
			}

			~BitVector()
			{
				Cleanup(true);
			}

			void Read(System::IO::BinaryReader^ reader, size_t count)
			{
				if (Capacity > 0)
				{
					throw gcnew System::InvalidOperationException("Cannot perform Read on a non-empty container");
				}

				EnsureCapacity(count);

				for (auto ix = 0; ix < count; ix += BITS_PER_ITEM)
				{
					SetGroup(ix, reader->ReadByte());
				}
			}

			void Write(System::IO::BinaryWriter^ writer, size_t count)
			{
				if (count > Capacity)
				{
					throw gcnew System::InvalidOperationException("Count to write is larger than capacity: " + count);
				}

				for (auto ix = 0; ix < count; ix += BITS_PER_ITEM)
				{
					byte group = GetGroup(ix);
					writer->Write(group);
				}
			}

			property size_t Capacity {
				[MethodImpl(MethodImplOptions::AggressiveInlining)]
				inline size_t get() { return m_itemCapacity * BITS_PER_ITEM; }
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline void ChangeAll(bool value)
			{
				auto nBlocks = m_pArray->capacity() / ITEMS_PER_BLOCK;
				dataarray_t::value_type newvalue = value ? ~0 : 0;
				for (auto p = m_pData; p != m_pData + nBlocks; p++)
				{
					for (auto v = *p; v != *p + ITEMS_PER_BLOCK; v++)
					{
						*v = newvalue;
					}
				}
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline void EnsureCapacity(size_t capacity)
			{
				if (!TryEnsureCapacity(capacity, System::Threading::Timeout::Infinite))
				{
					throw gcnew System::InsufficientMemoryException("Failed to increase capacity for " + capacity);
				}
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline void EnsureCapacity(int32_t capacity)
			{
				EnsureCapacity((size_t)capacity);
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline bool TryEnsureCapacity(size_t capacity)
			{
				return TryEnsureCapacity(capacity, 0);
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline bool TryEnsureCapacity(size_t capacity, System::Int32 timeout)
			{
				if (capacity > 0)
				{
					auto pdata = interior_ptr<dataarray_t::containerref_t>(&m_pData);
					auto pcapacity = interior_ptr<size_t>(&m_itemCapacity);
					return m_pArray->try_ensure_capacity(1 + capacity / BITS_PER_ITEM, timeout, pdata, pcapacity);
				}

				return true;
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline bool Get(size_t index)
			{
				auto pValue = m_pData[index / BITS_PER_BLOCK] + ((index / BITS_PER_ITEM) % ITEMS_PER_BLOCK);
				return 0 != (*pValue & (dataarray_t::value_type(1) << (index % BITS_PER_ITEM)));
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline dataarray_t::value_type GetGroup(size_t index)
			{
				auto pValue = m_pData[index / BITS_PER_BLOCK] + ((index / BITS_PER_ITEM) % ITEMS_PER_BLOCK);
				return *pValue;
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline void Set(size_t index)
			{
				auto pValue = m_pData[index / BITS_PER_BLOCK] + ((index / BITS_PER_ITEM) % ITEMS_PER_BLOCK);
				*pValue |= (dataarray_t::value_type(1) << (index % BITS_PER_ITEM));
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline void SetGroup(size_t index, dataarray_t::value_type group)
			{
				auto pValue = m_pData[index / BITS_PER_BLOCK] + ((index / BITS_PER_ITEM) % ITEMS_PER_BLOCK);
				*pValue = group;
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			[System::Security::SuppressUnmanagedCodeSecurityAttribute]
			inline void Clear(size_t index)
			{
				auto pValue = m_pData[index / BITS_PER_BLOCK] + ((index / BITS_PER_ITEM) % ITEMS_PER_BLOCK);
				*pValue &= ~(dataarray_t::value_type(1) << (index % BITS_PER_ITEM));
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline void SafeSet(size_t index)
			{
				auto pValue = m_pData[index / BITS_PER_BLOCK] + ((index / BITS_PER_ITEM) % ITEMS_PER_BLOCK);
				dataarray_t::value_type oldValue;
				dataarray_t::value_type value;
				do
				{
					oldValue = *pValue;
					value = oldValue | (dataarray_t::value_type(1) << (index % BITS_PER_ITEM));

					value = CAS(pValue, value, oldValue);
				} while (value != oldValue);
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline bool SafeGetAndSet(size_t index)
			{
				auto pValue = m_pData[index / BITS_PER_BLOCK] + ((index / BITS_PER_ITEM) % ITEMS_PER_BLOCK);
				dataarray_t::value_type oldValue;
				dataarray_t::value_type value;
				dataarray_t::value_type mask = dataarray_t::value_type(1) << (index % BITS_PER_ITEM);
				do
				{
					oldValue = *pValue;
					value = oldValue | mask;

					value = CAS(pValue, value, oldValue);
				} while (value != oldValue);

				return 0 != (value & mask);
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline void SafeClear(size_t index)
			{
				auto pValue = m_pData[index / BITS_PER_BLOCK] + ((index / BITS_PER_ITEM) % ITEMS_PER_BLOCK);
				dataarray_t::value_type oldValue;
				dataarray_t::value_type value;
				do
				{
					oldValue = *pValue;
					value = oldValue & ~(dataarray_t::value_type(1) << (index % BITS_PER_ITEM));

					value = CAS(pValue, value, oldValue);
				} while (value != oldValue);
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline bool SafeGetAndClear(size_t index)
			{
				auto pValue = m_pData[index / BITS_PER_BLOCK] + ((index / BITS_PER_ITEM) % ITEMS_PER_BLOCK);
				dataarray_t::value_type oldValue;
				dataarray_t::value_type value;
				dataarray_t::value_type mask = (dataarray_t::value_type(1) << (index % BITS_PER_ITEM));
				do
				{
					oldValue = *pValue;
					value = oldValue & ~mask;

					value = CAS(pValue, value, oldValue);
				} while (value != oldValue);

				return 0 != (value & mask);
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline bool Get(int32_t index)
			{
				return Get((size_t)index);
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline void Set(int32_t index)
			{
				Set((size_t)index);
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline void Clear(int32_t index)
			{
				Clear((size_t)index);
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline bool SafeGet(size_t index)
			{
				return Get(index);
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline bool SafeGet(int32_t index)
			{
				return Get(index);
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline void SafeSet(int32_t index)
			{
				SafeSet((size_t)index);
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline void SafeClear(int32_t index)
			{
				SafeClear((size_t)index);
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline bool SafeGetAndClear(int32_t index)
			{
				return SafeGetAndClear((size_t)index);
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline bool SafeGetAndSet(int32_t index)
			{
				return SafeGetAndSet((size_t)index);
			}
		};
	}
}