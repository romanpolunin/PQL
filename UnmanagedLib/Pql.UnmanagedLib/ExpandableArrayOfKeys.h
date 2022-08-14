#pragma once

#include <xstddef>
#include "MemoryPoolTypes.h"
#include "IUnmanagedAllocator.h"
#include "ExpandableArrayImpl.h"
#include "BitVector.h"

namespace Pql 
{
    namespace UnmanagedLib 
    {
        using namespace System::Runtime::CompilerServices;
#pragma managed
        public ref class ExpandableArrayOfKeys
        {
            using dataarray_t = ExpandableArrayImpl<uint8_t*>;
            using my_dataarray_allocator = zero_memory_pool_allocator<uint8_t>;

#define ITEMS_PER_BLOCK 65536
#define BLOCKS_GROWTH 64

            dataarray_t* _pArray;
            IUnmanagedAllocator^ _allocator;
            dataarray_t::containerref_t volatile _pData;
            size_t volatile _capacity;

            void Cleanup(bool disposing)
            {
                if (disposing)
                {
                    System::GC::SuppressFinalize(this);
                }

                _pData = nullptr;
                _capacity = 0;

                if (_pArray)
                {
                    for (auto x = 0; x < _pArray->capacity(); x++)
                    {
                        auto ptr = _pArray->get(x);
                        if (ptr)
                        {
                            _allocator->Free(ptr);
                        }
                    }

                    _pArray->~dataarray_t();
                    _allocator->Free(_pArray);
                    _pArray = nullptr;
                }
            }

            !ExpandableArrayOfKeys()
            {
                Cleanup(false);
            }

            void Initialize(ExpandableArrayOfKeys^ src, IUnmanagedAllocator^ allocator)
            {
                if (!allocator)
                {
                    throw gcnew System::ArgumentNullException("allocator");
                }

                _allocator = allocator;

                auto pobj = (dataarray_t*)_allocator->Alloc(sizeof(dataarray_t));

                _pArray = new (pobj)dataarray_t((MemoryPool*)_allocator->GetMemoryPool(), ITEMS_PER_BLOCK, BLOCKS_GROWTH);
            }

        public:

            ExpandableArrayOfKeys(IUnmanagedAllocator^ allocator)
            {
                Initialize(nullptr, allocator);
            }

            ExpandableArrayOfKeys(ExpandableArrayOfKeys^ src, IUnmanagedAllocator^ allocator)
            {
                Initialize(src, allocator);

                if (src)
                {
                    auto cap = src->Capacity;
                    EnsureCapacity(cap);

                    for (size_t ix = 0; ix < cap; ix++)
                    {
                        if (!TrySetAt(ix, src->GetAt(ix)))
                        {
                            throw gcnew System::InsufficientMemoryException("Could not copy element at " + ix);
                        }
                    }
                }
            }

            ~ExpandableArrayOfKeys()
            {
                Cleanup(true);
            }

            void Read(System::IO::BinaryReader^ reader, size_t count, BitVector^ validEntries)
            {
                if (Capacity > 0)
                {
                    throw gcnew System::InvalidOperationException("Cannot perform Read on a non-empty container");
                }

                if (validEntries == nullptr)
                {
                    throw gcnew System::ArgumentNullException("validEntries");
                }

                if (reader == nullptr)
                {
                    throw gcnew System::ArgumentNullException("reader");
                }

                EnsureCapacity(count);

                uint8_t buff[255];

                for (size_t ix = 0; ix < count; ix++)
                {
                    if (!validEntries->Get(ix))
                    {
                        continue;
                    }

                    buff[0] = reader->ReadByte();
                    bool result;

                    if (buff[0] == 0)
                    {
                        result = TrySetAt(ix, (uint8_t*)nullptr);
                    }
                    else
                    {
                        for (auto c = 1; c <= buff[0]; c++)
                        {
                            buff[c] = reader->ReadByte();
                        }

                        result = TrySetAt(ix, buff);
                    }

                    if (!result)
                    {
                        throw gcnew System::Exception("Failed to append new value at " + ix);
                    }
                }
            }

            void Write(System::IO::BinaryWriter^ writer, size_t count, BitVector^ validEntries)
            {
                if (writer == nullptr)
                {
                    throw gcnew System::ArgumentNullException("writer");
                }

                if (validEntries == nullptr)
                {
                    throw gcnew System::ArgumentNullException("validEntries");
                }

                if (count > Capacity)
                {
                    throw gcnew System::InvalidOperationException("Count to write is larger than capacity: " + count);
                }

                for (size_t ix = 0; ix < count; ix++)
                {
                    if (!validEntries->Get(ix))
                    {
                        continue;
                    }

                    auto value = GetAt(ix);
                    if (value)
                    {
                        writer->Write(uint8_t(value[0]));
                        for (auto c = 1; c <= value[0]; c++)
                        {
                            writer->Write(uint8_t(value[c]));
                        }
                    }
                    else
                    {
                        writer->Write(uint8_t(0));
                    }
                }
            }

            property size_t Capacity {
                inline size_t get() { return _capacity; }
            }

            inline void EnsureCapacity(int32_t capacity)
            {
                EnsureCapacity((size_t)capacity);
            }

            inline void EnsureCapacity(size_t capacity)
            {
                if (!TryEnsureCapacity(capacity, System::Threading::Timeout::Infinite))
                {
                    throw gcnew System::InsufficientMemoryException("Failed to ensure capacity for " + capacity);
                }
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
                    auto pcapacity = interior_ptr<size_t>(&_capacity);
                    auto result = _pArray->try_ensure_capacity(capacity, timeout, pdata, pcapacity);
                }

                return true;
            }

            inline bool TrySetAt(int32_t index, array<uint8_t>^ data)
            {
                if (data == nullptr)
                {
                    return TrySetAt(index, (uint8_t*)nullptr);
                }

                auto datalen = data->Length;
                if (datalen < 2)
                {
                    throw gcnew System::ArgumentException("Key must have at least one byte for size, plus one byte for value");
                }

                pin_ptr<uint8_t> pdata = &data[0];

                uint8_t contentlen = pdata[0];
                if (contentlen > datalen)
                {
                    throw gcnew System::ArgumentOutOfRangeException("contentlen", contentlen, "Key length prefix byte must be less than array length");
                }

                return TrySetAt((size_t)index, pdata);
            }

            inline bool TrySetAt(size_t index, uint8_t* pdata)
            {
                if (index >= Capacity)
                {
                    throw gcnew System::ArgumentOutOfRangeException("index", index, "Index must be less than allocated capacity");
                }

                void* pnew;
                if (pdata != nullptr)
                {
                    uint8_t contentlen = pdata[0];
                    if (contentlen == 0)
                    {
                        throw gcnew System::ArgumentOutOfRangeException("contentlen", contentlen, "Key length prefix byte must be positive");
                    }

                    pnew = _allocator->Alloc(contentlen + 1);

                    memcpy(pnew, (void*)pdata, contentlen + 1);
                }
                else
                {
                    pnew = nullptr;
                }

                void* prev;

                auto targetref = _pData[index / ITEMS_PER_BLOCK] + (index % ITEMS_PER_BLOCK);
                prev = *targetref;
                if (prev != UnmanagedLib_InterlockedCompareExchangePointer((void* volatile*)targetref, (void*)pnew, prev))
                {
                    // somebody else just updated the same entry, discard our work here
                    if (pnew)
                    {
                        _allocator->Free(pnew);
                    }
                    return false;
                }

                if (prev)
                {
                    _allocator->ScheduleForCollection(prev);
                }

                return true;
            }

            inline dataarray_t::value_type GetAt(size_t index)
            {
                if (index >= Capacity)
                {
                    throw gcnew System::ArgumentOutOfRangeException("index", index, "Index must be less than allocated capacity");
                }

                return *(_pData[index / ITEMS_PER_BLOCK] + (index % ITEMS_PER_BLOCK));
            }

            inline uint8_t GetAt(int32_t index, array<uint8_t>^ data)
            {
                if (data == nullptr)
                {
                    throw gcnew System::ArgumentNullException("data");
                }

                auto value = GetAt(index);

                auto bytecount = value[0] + 1;
                if (bytecount > data->Length)
                {
                    throw gcnew System::ArgumentException("Buffer is too small, must have: " + bytecount, "data");
                }

                pin_ptr<uint8_t> pdata = &data[0];
                memcpy(pdata, value, bytecount);
                return bytecount;
            }

            inline System::IntPtr GetIntPtrAt(int32_t index)
            {
                if (index >= Capacity)
                {
                    throw gcnew System::ArgumentOutOfRangeException("index", index, "Index must be less than allocated capacity");
                }

                return System::IntPtr(*(_pData[index / ITEMS_PER_BLOCK] + (index % ITEMS_PER_BLOCK)));
            }
        };
    }
}