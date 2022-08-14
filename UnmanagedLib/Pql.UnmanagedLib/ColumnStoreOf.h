#pragma once

#include "IUnmanagedAllocator.h"
#include "ExpandableArrayImpl.h"
#include "BitVector.h"

namespace Pql
{
    namespace UnmanagedLib_ColumnStore
    {
        template <class T>
        public ref class ColumnStoreOf
        {
            typedef UnmanagedLib::ExpandableArrayImpl<T> dataarray_t;
            dataarray_t* _pArray;
            UnmanagedLib::IUnmanagedAllocator^ _allocator;
            UnmanagedLib::BitVector^ _notNulls;

            void Initialize(ColumnStoreOf^ src, UnmanagedLib::IUnmanagedAllocator^ allocator)
            {
                if (!allocator)
                {
                    throw gcnew System::ArgumentNullException("allocator");
                }

                _allocator = allocator;

                DestroyDealloc(_pArray, allocator->GetAllocator());

                if (src)
                {
                }
            }

        protected:
            ColumnStoreOf(void)
            {
                throw gcnew System::Exception("This constructor is not supposed to run, it is only used as a placeholder to enforce template members instantiation");

                // This "false" block is here to force compiler to instantiate template methods in the descendant class,
                // even though they are not explicitly referenced by that inheritor's code.
                // Another way to do that would be declaring those methods virtual, paying additional runtime cost.
                if (false)
                {
                    Get(0);
                    Set(0, T());
                    IsNotNull(0);
                    ClearIsNotNull(0);
                    SetIsNotNull(0);
                }
            }

        public:

            ColumnStoreOf(UnmanagedLib::IUnmanagedAllocator^ allocator)
            {
                Initialize(nullptr, allocator);
            }

            ColumnStoreOf(ColumnStoreOf^ src, UnmanagedLib::IUnmanagedAllocator^ allocator)
            {
                if (!src)
                {
                    throw gcnew System::ArgumentNullException("src");
                }

                Initialize(src, allocator);
            }

            [MethodImpl(MethodImplOptions::AggressiveInlining)]
            void EnsureCapacity(size_t capacity)
            {
                _notNulls->EnsureCapacity(capacity);
                if (!_pArray->try_ensure_capacity(capacity))
                {
                    throw gcnew System::Exception("Failed to ensure capacity for " + capacity);
                }
            }

            [MethodImpl(MethodImplOptions::AggressiveInlining)]
            inline T Get(size_t index)
            {
                auto pval = (T*)_pArray->reference(index * sizeof(T));
                return *pval;
            }

            [MethodImpl(MethodImplOptions::AggressiveInlining)]
            inline void Set(size_t index, T value)
            {

            }

            [MethodImpl(MethodImplOptions::AggressiveInlining)]
            inline bool IsNotNull(size_t index)
            {
                return _notNulls->Get(index);
            }

            [MethodImpl(MethodImplOptions::AggressiveInlining)]
            inline void ClearIsNotNull(size_t index)
            {
                _notNulls->Clear(index);
            }

            [MethodImpl(MethodImplOptions::AggressiveInlining)]
            inline void SetIsNotNull(size_t index)
            {
                _notNulls->Set(index);
            }
        };
    }

#define EXPANDABLE_ARRAY_OF(__TYPE__)  \
	public ref struct ColumnStoreOf ## __TYPE__ : public UnmanagedLib_ColumnStore::ColumnStoreOf<System::__TYPE__> { \
		ColumnStoreOf ## __TYPE__() : ColumnStoreOf() {}  \
	public:\
		ColumnStoreOf ## __TYPE__(UnmanagedLib::IUnmanagedAllocator^ allocator) : ColumnStoreOf(allocator) {} \
		ColumnStoreOf ## __TYPE__(ColumnStoreOf ## __TYPE__ ^src, UnmanagedLib::IUnmanagedAllocator^ allocator) : ColumnStoreOf(src, allocator) {} \
	};

    EXPANDABLE_ARRAY_OF(Byte);
    EXPANDABLE_ARRAY_OF(SByte);
    EXPANDABLE_ARRAY_OF(Int16);
    EXPANDABLE_ARRAY_OF(Int32);
    EXPANDABLE_ARRAY_OF(Int64);
    EXPANDABLE_ARRAY_OF(UInt16);
    EXPANDABLE_ARRAY_OF(UInt32);
    EXPANDABLE_ARRAY_OF(UInt64);
    EXPANDABLE_ARRAY_OF(Single);
    EXPANDABLE_ARRAY_OF(Double);
    EXPANDABLE_ARRAY_OF(Decimal);
    EXPANDABLE_ARRAY_OF(DateTime);
    EXPANDABLE_ARRAY_OF(DateTimeOffset);
    EXPANDABLE_ARRAY_OF(TimeSpan);
    EXPANDABLE_ARRAY_OF(Guid);
#undef EXPANDABLE_ARRAY_OF

    public ref class ColumnStoreFactory abstract sealed
    {
    public:

        static System::Object^ Create(System::Type^ valueType, System::Object^ tocopy, UnmanagedLib::IUnmanagedAllocator^ allocator)
        {
            if (!valueType)
            {
                throw gcnew System::ArgumentNullException("valueType");
            }

            auto arrayType = System::Type::GetType("UnmanagedLib.ColumnStoreOf" + valueType->Name);
            if (!arrayType)
            {
                throw gcnew System::ArgumentException("Expandable array does not exist for " + valueType->Name);
            }

            if (tocopy)
            {
                auto ctr = arrayType->GetConstructor(gcnew array<System::Type^> { arrayType, UnmanagedLib::IUnmanagedAllocator::typeid });
                if (!ctr)
                {
                    throw gcnew System::Exception("Could not find constructor to match specified arguments");
                }

                return ctr->Invoke(gcnew array<System::Object^> { tocopy, allocator });
            }
            else
            {
                auto ctr = arrayType->GetConstructor(gcnew array<System::Type^> { UnmanagedLib::IUnmanagedAllocator::typeid });
                if (!ctr)
                {
                    throw gcnew System::Exception("Could not find constructor to match specified arguments");
                }

                return ctr->Invoke(gcnew array<System::Object^> { allocator });
            }
        }
    };
}
