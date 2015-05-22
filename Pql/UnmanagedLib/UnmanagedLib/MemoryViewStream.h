#pragma once

#include <cstdint>
#include <xstddef>

namespace Pql {
	namespace UnmanagedLib {

		public ref class MemoryViewStream : System::IO::Stream
		{
			bool m_disposed;
			int64_t m_bytesInBuffer;
			int64_t m_positionInBuffer;
			byte* m_buffer;

			inline void CheckDisposed()
			{
				if (m_disposed)
				{
					throw gcnew System::ObjectDisposedException("MemoryViewStream");
				}
			}

		public:

			MemoryViewStream(void)
			{
			}

			void Attach(byte* p, size_t len)
			{
				if (p == nullptr)
				{
					throw gcnew System::ArgumentNullException("p");
				}

				m_buffer = p;
				m_bytesInBuffer = len;
				m_positionInBuffer = 0;
			}

			~MemoryViewStream() override
			{
				m_disposed = true;
				m_buffer = nullptr;
				m_positionInBuffer = 0;
				m_bytesInBuffer = 0;
			}

			virtual void Flush() override
			{
				CheckDisposed();
			}

			virtual int64_t Seek(int64_t offset, System::IO::SeekOrigin origin) override
			{
				CheckDisposed();

				int64_t newpos;
				if (origin == System::IO::SeekOrigin::Current)
				{
					newpos = m_positionInBuffer + offset;
				}
				else if (origin == System::IO::SeekOrigin::Begin)
				{
					newpos = offset;
				}
				else if (origin == System::IO::SeekOrigin::End)
				{
					newpos = m_bytesInBuffer - offset;
				}
				else
				{
					throw gcnew System::ArgumentException("origin", "Invalid value");
				}

				if (newpos < 0)
				{
					newpos = 0;
				}
				else if (newpos > m_bytesInBuffer)
				{
					newpos = m_bytesInBuffer;
				}

				m_positionInBuffer = newpos;
				return m_positionInBuffer;
			}

			virtual void SetLength(int64_t value) override
			{
				throw gcnew System::NotSupportedException();
			}

			virtual int32_t Read(array<byte>^ buffer, int32_t offset, int32_t count) override
			{
				CheckDisposed();

				if (buffer == nullptr)
				{
					throw gcnew System::ArgumentNullException("buffer");
				}

				if (count < 0)
				{
					throw gcnew System::ArgumentOutOfRangeException("count", "count is negative");
				}

				if (offset < 0)
				{
					throw gcnew System::ArgumentOutOfRangeException("offset", "offset is negative");
				}

				if (offset + count > buffer->Length)
				{
					throw gcnew System::ArgumentException("The sum of offset and count is larger than the buffer length.");
				}

				int32_t bytesRead = (int32_t)(min(count, m_bytesInBuffer - m_positionInBuffer));

				if (bytesRead > 0)
				{
					for (auto head = m_buffer + m_positionInBuffer; head != (m_buffer + m_positionInBuffer + bytesRead); head++, offset++)
					{
						buffer[offset] = *head;
					}

					m_positionInBuffer += bytesRead;
				}

				return bytesRead;
			}

			virtual void Write(array<byte>^ buffer, int32_t offset, int32_t count) override
			{
				CheckDisposed();

				if (buffer == nullptr)
				{
					throw gcnew System::ArgumentNullException("buffer");
				}

				if (count < 0)
				{
					throw gcnew System::ArgumentOutOfRangeException("count", "count is negative");
				}

				if (offset < 0)
				{
					throw gcnew System::ArgumentOutOfRangeException("offset", "offset is negative");
				}

				if (offset + count > buffer->Length)
				{
					throw gcnew System::ArgumentException("The sum of offset and count is larger than the buffer length.");
				}

				if (count > m_bytesInBuffer - m_positionInBuffer)
				{
					throw gcnew System::IO::IOException("Insufficient space to write this number of bytes: " + count);
				}

				for (auto p = m_buffer + m_positionInBuffer; p < m_buffer + m_positionInBuffer + count; p++, offset++)
				{
					*p = buffer[offset];
				}

				m_positionInBuffer += count;
			}

			/// <summary>
			/// Returns true.
			/// </summary>
			/// <returns>
			/// true if the stream supports reading; otherwise, false.
			/// </returns>
			/// <filterpriority>1</filterpriority>
			property virtual bool CanRead
			{
				bool get() override { return true; }
			}

			/// <summary>
			/// Returns false.
			/// </summary>
			/// <returns>
			/// true if the stream supports seeking; otherwise, false.
			/// </returns>
			/// <filterpriority>1</filterpriority>
			property virtual bool CanSeek
			{
				bool get() override { return true; }
			}

			/// <summary>
			/// Returns false.
			/// </summary>
			/// <returns>
			/// true if the stream supports writing; otherwise, false.
			/// </returns>
			/// <filterpriority>1</filterpriority>
			property virtual bool CanWrite
			{
				bool get() override { return true; }
			}

			/// <summary>
			/// Not supported.
			/// </summary>
			property virtual int64_t Length
			{
				int64_t get() override { return m_bytesInBuffer; }
			}

			/// <summary>
			/// Not supported.
			/// </summary>
			property virtual int64_t Position
			{
				int64_t get() override { return m_positionInBuffer; }
				void set(int64_t pos) override { Seek(pos, System::IO::SeekOrigin::Begin); }
			}
		};
	}
}