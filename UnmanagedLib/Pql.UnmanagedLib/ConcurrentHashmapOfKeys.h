#pragma once
#pragma unmanaged

#include <xstddef>
#include <algorithm>
#include <functional>
#include "oneapi/tbb/concurrent_unordered_map.h"
#include "MemoryPoolTypes.h"

#pragma managed
#include "IUnmanagedAllocator.h"

namespace Pql
{
    namespace UnmanagedLib
    {
        namespace ConcurrentHashMapOfKeysNamespace 
        {
#pragma unmanaged
            typedef const uint8_t* key_t;   // keys are length-prefixed byte arrays
            typedef uint64_t value_t;

            template <typename K> struct SequenceHash 
            {
                // hash functor for plain old data
                size_t operator()(const K& _keyval) const
                {
                    // first byte must hold length
                    //return (std::_Hash_seq(_keyval + 1, _keyval[0]));

                    auto hash = std::_Hash_array_representation(_keyval + 1, _keyval[0]);
                    //for (auto i = 0; i <= _keyval[0]; i++)
                    //{
                    //	System::Console::Write(_keyval[i] + ", ");
                    //}
                    //System::Console::WriteLine(", Hash value: " + hash);
                    return hash;
                }
            };

            template<typename K> struct SequenceEqualTo
            {
                bool operator()(const K& _Left, const K& _Right) const
                {
                    auto _len = _Left[0];
                    if (_len != _Right[0])
                    {
                        return false;
                    }

                    for (auto _left = _Left + 1, _leftend = _left + _len, _right = _Right + 1;
                        _left != _leftend; _left++, _right++)
                    {
                        //System::Console::Write(*_left + ":" + *_right + "; ");

                        if ((*_left) != (*_right))
                        {
                            return false;
                        }
                    }

                    return true;
                }
            };
        }

        using namespace ConcurrentHashMapOfKeysNamespace;

        using my_hashmap_allocator_t = zero_memory_pool_allocator<std::pair<const key_t, value_t>>;
        using hashmap_t = tbb::concurrent_unordered_map<key_t, value_t, SequenceHash<key_t>, SequenceEqualTo<key_t>, my_hashmap_allocator_t>;
        
#pragma managed
        public ref class ConcurrentHashmapOfKeys
        {

        private:
            
        private:
            hashmap_t* _pMap;
            IUnmanagedAllocator^ _allocator;

            void Cleanup(bool disposing)
            {
                if (disposing)
                {
                    System::GC::SuppressFinalize(this);
                }

                if (_pMap)
                {
                    _pMap->~hashmap_t();
                    _allocator->Free(_pMap);
                    _pMap = nullptr;
                }
            }

            !ConcurrentHashmapOfKeys()
            {
                Cleanup(false);
            }

            void Initialize(ConcurrentHashmapOfKeys^ src, ExpandableArrayOfKeys^ srcValues, IUnmanagedAllocator^ allocator)
            {
                if (!allocator)
                {
                    throw gcnew System::ArgumentNullException("allocator");
                }

                _allocator = allocator;

                _pMap = (hashmap_t*)_allocator->Alloc(sizeof(hashmap_t));
                _pMap = new (_pMap)hashmap_t(my_hashmap_allocator_t((((MemoryPool*)_allocator->GetMemoryPool())->get_pool())));

                if (src != nullptr)
                {
                    for (auto it = src->_pMap->cbegin(), end = src->_pMap->cend(); it != end; it++)
                    {
                        auto value = it->second;
                        auto key = srcValues->GetAt(value);
                        _pMap->insert(hashmap_t::value_type(key, value));
                    }
                }
            }

        public:

            ConcurrentHashmapOfKeys(IUnmanagedAllocator^ allocator)
            {
                Initialize(nullptr, nullptr, allocator);
            }

            ConcurrentHashmapOfKeys(ConcurrentHashmapOfKeys^ src, ExpandableArrayOfKeys^ srcValues, IUnmanagedAllocator^ allocator)
            {
                Initialize(src, srcValues, allocator);
            }

            ~ConcurrentHashmapOfKeys()
            {
                Cleanup(true);
            }

            inline bool TryAdd(uint8_t* key, System::UInt64 value)
            {
                if (key == nullptr)
                {
                    throw gcnew System::ArgumentNullException("key");
                }

                if (key[0] == 0)
                {
                    throw gcnew System::ArgumentException("Key length prefix byte must be positive");
                }

                try
                {
                    auto p = _pMap->insert(hashmap_t::value_type(key, value));
                    return p.second;
                }
                catch (System::InsufficientMemoryException^)
                {
                    return false;
                }
            }

            inline bool TryAdd(System::IntPtr key, int32_t value)
            {
                if (key == System::IntPtr::Zero)
                {
                    throw gcnew System::ArgumentNullException("key");
                }

                auto pkey = (uint8_t*)(void*)key;
                if (pkey[0] == 0)
                {
                    throw gcnew System::ArgumentException("Key length prefix byte must be positive");
                }

                try
                {
                    auto p = _pMap->insert(hashmap_t::value_type(pkey, value));
                    return p.second;
                }
                catch (System::InsufficientMemoryException^)
                {
                    return false;
                }
            }

            inline value_t GetAt(array<uint8_t>^ key)
            {
                value_t result;
                if (!TryGetValue(key, result))
                {
                    throw gcnew System::Collections::Generic::KeyNotFoundException();
                }

                return result;
            }

            inline int32_t GetInt32At(array<uint8_t>^ key)
            {
                return (int32_t)GetAt(key);
            }

            inline bool TryGetValueInt32(array<uint8_t>^ key, int32_t% value)
            {
                value_t result;
                if (TryGetValue(key, result))
                {
                    value = (int32_t)result;
                    return true;
                }

                value = 0;
                return false;
            }

            inline bool TryGetValue(array<uint8_t>^ key, value_t% value)
            {
                if (key == nullptr)
                {
                    throw gcnew System::ArgumentNullException("key");
                }

                if (key->Length < 2)
                {
                    throw gcnew System::ArgumentException("Key must have at least one byte for size, plus one byte for value");
                }

                pin_ptr<uint8_t> p = &key[0];

                auto len = p[0];
                if (len == 0 || len > key->Length - 1)
                {
                    throw gcnew System::ArgumentOutOfRangeException("len", len, "Key length prefix byte must be positive and less than array length");
                }

                auto it = _pMap->find((uint8_t*)p);
                if (it == _pMap->end())
                {
                    return false;
                }

                value = it->second;
                return true;
            }

            inline bool TryGetValue(uint8_t* key, value_t% value)
            {
                if (key == nullptr)
                {
                    throw gcnew System::ArgumentNullException("key");
                }

                auto len = key[0];

                if (len == 0)
                {
                    throw gcnew System::ArgumentException("Key length prefix byte must be positive");
                }

                auto it = _pMap->find(key);
                if (it == _pMap->end())
                {
                    return false;
                }

                value = it->second;
                return true;
            }

            inline void Clear()
            {
                _pMap->clear();
            }

        };
    }
}