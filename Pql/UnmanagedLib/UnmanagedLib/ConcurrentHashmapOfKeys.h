#pragma once

#include <xstddef>
#include "tbb/concurrent_unordered_map.h"
#include "MemoryPoolTypes.h"
#include "IUnmanagedAllocator.h"

namespace Pql {
	namespace UnmanagedLib  {

		namespace ConcurrentHashMapOfKeysNamespace {

			typedef const uint8_t* key_t;   // keys are length-prefixed byte arrays
			typedef uint64_t value_t;

			template <typename K> struct SequenceHash : public std::unary_function < K&, size_t >
			{
				// hash functor for plain old data
				size_t operator()(const K& _keyval) const
				{
					// first byte must hold length
					//return (std::_Hash_seq(_keyval + 1, _keyval[0]));

					auto hash = std::_Hash_seq(_keyval + 1, _keyval[0]);
					//for (auto i = 0; i <= _keyval[0]; i++)
					//{
					//	System::Console::Write(_keyval[i] + ", ");
					//}
					//System::Console::WriteLine(", Hash value: " + hash);
					return hash;
				}
			};

			template<typename K> struct SequenceEqualTo : public std::binary_function < K, K, bool >
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

		public ref class ConcurrentHashmapOfKeys
		{

		public:
			typedef tbb::concurrent_unordered_map<key_t, value_t, SequenceHash<key_t>, SequenceEqualTo<key_t>, memorypoolallocator_t> hashmap_t;

		private:
			hashmap_t* m_pMap;
			IUnmanagedAllocator^ m_allocator;

			void Cleanup(bool disposing)
			{
				if (disposing)
				{
					System::GC::SuppressFinalize(this);
				}

				if (m_pMap)
				{
					m_pMap->~hashmap_t();
					m_allocator->Free(m_pMap);
					m_pMap = nullptr;
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

				m_allocator = allocator;

				auto pobj = (hashmap_t*)m_allocator->Alloc(sizeof(hashmap_t));

				m_pMap = new (pobj)hashmap_t(*m_allocator->GetAllocator());
				if (src != nullptr)
				{
					for (auto it = src->m_pMap->cbegin(), end = src->m_pMap->cend(); it != end; it++)
					{
						auto value = it->second;
						auto key = srcValues->GetAt(value);
						m_pMap->insert(hashmap_t::value_type(key, value));
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

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			[System::Runtime::ExceptionServices::HandleProcessCorruptedStateExceptions]
			inline bool TryAdd(byte* key, uint64_t value)
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
					auto p = m_pMap->insert(hashmap_t::value_type(key, value));
					return p.second;
				}
				catch (System::InsufficientMemoryException^)
				{
					return false;
				}
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			[System::Runtime::ExceptionServices::HandleProcessCorruptedStateExceptions]
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
					auto p = m_pMap->insert(hashmap_t::value_type(pkey, value));
					return p.second;
				}
				catch (System::InsufficientMemoryException^)
				{
					return false;
				}
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline value_t GetAt(array<byte>^ key)
			{
				value_t result;
				if (!TryGetValue(key, result))
				{
					throw gcnew System::Collections::Generic::KeyNotFoundException();
				}

				return result;
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline int32_t GetInt32At(array<byte>^ key)
			{
				return (int32_t)GetAt(key);
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline bool TryGetValueInt32(array<byte>^ key, int32_t% value)
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

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			[System::Runtime::ExceptionServices::HandleProcessCorruptedStateExceptions]
			inline bool TryGetValue(array<byte>^ key, value_t% value)
			{
				if (key == nullptr)
				{
					throw gcnew System::ArgumentNullException("key");
				}

				if (key->Length < 2)
				{
					throw gcnew System::ArgumentException("Key must have at least one byte for size, plus one byte for value");
				}

				pin_ptr<byte> p = &key[0];

				auto len = p[0];
				if (len == 0 || len > key->Length - 1)
				{
					throw gcnew System::ArgumentOutOfRangeException("len", len, "Key length prefix byte must be positive and less than array length");
				}

				auto it = m_pMap->find((uint8_t*)p);
				if (it == m_pMap->end())
				{
					return false;
				}

				value = it->second;
				return true;
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			[System::Runtime::ExceptionServices::HandleProcessCorruptedStateExceptions]
			inline bool TryGetValue(byte* key, value_t% value)
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

				auto it = m_pMap->find(key);
				if (it == m_pMap->end())
				{
					return false;
				}

				value = it->second;
				return true;
			}

			[MethodImpl(MethodImplOptions::AggressiveInlining)]
			inline void Clear()
			{
				m_pMap->clear();
			}

		};
	}
}