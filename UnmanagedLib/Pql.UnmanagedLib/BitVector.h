#pragma once

#include <memory>
#include "IUnmanagedAllocator.h"
#include "ExpandableArrayImpl.h"

namespace Pql
{
    namespace UnmanagedLib
    {
        using namespace System::Runtime::CompilerServices;
#pragma managed
        public ref class BitVector
        {
            using dataarray_t = ExpandableArrayImpl<uint8_t>;

#define ITEMS_PER_BLOCK 65536
#define BITS_PER_ITEM 8
#define BITS_PER_BLOCK (ITEMS_PER_BLOCK * BITS_PER_ITEM)
#define BLOCKS_GROWTH 64
#define CAS UnmanagedLib_InterlockedCompareExchange8

            IUnmanagedAllocator^ _allocator;
            dataarray_t* _pArray;
            dataarray_t::containerref_t volatile _pData;
            size_t volatile _itemCapacity;

            void Cleanup(bool disposing)
            {
                if (disposing)
                {
                    System::GC::SuppressFinalize(this);
                }

                // simply discard the reference
                // rely upon pool management to clean up the garbage
                _pArray = nullptr;
                _pData = nullptr;
                _itemCapacity = 0;
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

                _allocator = allocator;

                auto pobj = (dataarray_t*)_allocator->Alloc(sizeof(dataarray_t));

                _pArray = new (pobj)dataarray_t((MemoryPool*)_allocator->GetMemoryPool(), ITEMS_PER_BLOCK, BLOCKS_GROWTH);

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
                    System::Byte group = GetGroup(ix);
                    writer->Write(group);
                }
            }

            property size_t Capacity
            {
                inline size_t get() { return _itemCapacity * BITS_PER_ITEM; }
            }

            inline void ChangeAll(bool value)
            {
                auto nBlocks = _pArray->capacity() / ITEMS_PER_BLOCK;
                dataarray_t::value_type newvalue = value ? ~0 : 0;
                for (auto p = _pData; p != _pData + nBlocks; p++)
                {
                    for (auto v = *p; v != *p + ITEMS_PER_BLOCK; v++)
                    {
                        *v = newvalue;
                    }
                }
            }

            inline void EnsureCapacity(size_t capacity)
            {
                if (!TryEnsureCapacity(capacity, System::Threading::Timeout::Infinite))
                {
                    throw gcnew System::InsufficientMemoryException("Failed to increase capacity for " + capacity);
                }
            }

            inline void EnsureCapacity(int32_t capacity)
            {
                EnsureCapacity((size_t)capacity);
            }

            inline bool TryEnsureCapacity(size_t capacity)
            {
                return TryEnsureCapacity(capacity, 0);
            }

            inline bool TryEnsureCapacity(size_t capacity, System::Int32 timeout)
            {
                if (capacity > 0)
                {
                    auto pdata = interior_ptr<dataarray_t::containerref_t>(&_pData);
                    auto pcapacity = interior_ptr<size_t>(&_itemCapacity);
                    return _pArray->try_ensure_capacity(1 + capacity / BITS_PER_ITEM, timeout, pdata, pcapacity);
                }

                return true;
            }

            inline bool Get(size_t index)
            {
                auto pValue = _pData[index / BITS_PER_BLOCK] + ((index / BITS_PER_ITEM) % ITEMS_PER_BLOCK);
                return 0 != (*pValue & (dataarray_t::value_type(1) << (index % BITS_PER_ITEM)));
            }

            inline dataarray_t::value_type GetGroup(size_t index)
            {
                auto pValue = _pData[index / BITS_PER_BLOCK] + ((index / BITS_PER_ITEM) % ITEMS_PER_BLOCK);
                return *pValue;
            }

            inline void Set(size_t index)
            {
                auto pValue = _pData[index / BITS_PER_BLOCK] + ((index / BITS_PER_ITEM) % ITEMS_PER_BLOCK);
                *pValue |= (dataarray_t::value_type(1) << (index % BITS_PER_ITEM));
            }

            inline void SetGroup(size_t index, dataarray_t::value_type group)
            {
                auto pValue = _pData[index / BITS_PER_BLOCK] + ((index / BITS_PER_ITEM) % ITEMS_PER_BLOCK);
                *pValue = group;
            }

            inline void Clear(size_t index)
            {
                auto pValue = _pData[index / BITS_PER_BLOCK] + ((index / BITS_PER_ITEM) % ITEMS_PER_BLOCK);
                *pValue &= ~(dataarray_t::value_type(1) << (index % BITS_PER_ITEM));
            }

            inline void SafeSet(size_t index)
            {
                auto pValue = _pData[index / BITS_PER_BLOCK] + ((index / BITS_PER_ITEM) % ITEMS_PER_BLOCK);
                dataarray_t::value_type oldValue;
                dataarray_t::value_type value;
                do
                {
                    oldValue = *pValue;
                    value = oldValue | (dataarray_t::value_type(1) << (index % BITS_PER_ITEM));

                    value = CAS(pValue, value, oldValue);
                } while (value != oldValue);
            }

            inline bool SafeGetAndSet(size_t index)
            {
                auto pValue = _pData[index / BITS_PER_BLOCK] + ((index / BITS_PER_ITEM) % ITEMS_PER_BLOCK);
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

            inline void SafeClear(size_t index)
            {
                auto pValue = _pData[index / BITS_PER_BLOCK] + ((index / BITS_PER_ITEM) % ITEMS_PER_BLOCK);
                dataarray_t::value_type oldValue;
                dataarray_t::value_type value;
                do
                {
                    oldValue = *pValue;
                    value = oldValue & ~(dataarray_t::value_type(1) << (index % BITS_PER_ITEM));

                    value = CAS(pValue, value, oldValue);
                } while (value != oldValue);
            }

            inline bool SafeGetAndClear(size_t index)
            {
                auto pValue = _pData[index / BITS_PER_BLOCK] + ((index / BITS_PER_ITEM) % ITEMS_PER_BLOCK);
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

            inline bool Get(int32_t index)
            {
                return Get((size_t)index);
            }

            inline void Set(int32_t index)
            {
                Set((size_t)index);
            }

            inline void Clear(int32_t index)
            {
                Clear((size_t)index);
            }

            inline bool SafeGet(size_t index)
            {
                return Get(index);
            }

            inline bool SafeGet(int32_t index)
            {
                return Get(index);
            }

            inline void SafeSet(int32_t index)
            {
                SafeSet((size_t)index);
            }

            inline void SafeClear(int32_t index)
            {
                SafeClear((size_t)index);
            }

            inline bool SafeGetAndClear(int32_t index)
            {
                return SafeGetAndClear((size_t)index);
            }

            inline bool SafeGetAndSet(int32_t index)
            {
                return SafeGetAndSet((size_t)index);
            }
        };
    }
}