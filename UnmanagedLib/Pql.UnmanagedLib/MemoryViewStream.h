#pragma once

#include <cstdint>
#include <xstddef>
#include <algorithm>

namespace Pql {
    namespace UnmanagedLib {

        public ref class MemoryViewStream : System::IO::Stream
        {
            bool _disposed;
            int64_t _bytesInBuffer;
            int64_t _positionInBuffer;
            System::Byte* _buffer;

            inline void CheckDisposed()
            {
                if (_disposed)
                {
                    throw gcnew System::ObjectDisposedException("MemoryViewStream");
                }
            }

        public:

            MemoryViewStream(void)
            {
            }

            void Attach(System::Byte* p, size_t len)
            {
                if (p == nullptr)
                {
                    throw gcnew System::ArgumentNullException("p");
                }

                _buffer = p;
                _bytesInBuffer = len;
                _positionInBuffer = 0;
            }

            ~MemoryViewStream() override
            {
                _disposed = true;
                _buffer = nullptr;
                _positionInBuffer = 0;
                _bytesInBuffer = 0;
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
                    newpos = _positionInBuffer + offset;
                }
                else if (origin == System::IO::SeekOrigin::Begin)
                {
                    newpos = offset;
                }
                else if (origin == System::IO::SeekOrigin::End)
                {
                    newpos = _bytesInBuffer - offset;
                }
                else
                {
                    throw gcnew System::ArgumentException("origin", "Invalid value");
                }

                if (newpos < 0)
                {
                    newpos = 0;
                }
                else if (newpos > _bytesInBuffer)
                {
                    newpos = _bytesInBuffer;
                }

                _positionInBuffer = newpos;
                return _positionInBuffer;
            }

            virtual void SetLength(int64_t value) override
            {
                throw gcnew System::NotSupportedException();
            }

            virtual int32_t Read(array<System::Byte>^ buffer, int32_t offset, int32_t count) override
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

                int32_t bytesRead = (int32_t)(std::min(count, int32_t(_bytesInBuffer - _positionInBuffer)));

                if (bytesRead > 0)
                {
                    for (auto head = _buffer + _positionInBuffer; head != (_buffer + _positionInBuffer + bytesRead); head++, offset++)
                    {
                        buffer[offset] = *head;
                    }

                    _positionInBuffer += bytesRead;
                }

                return bytesRead;
            }

            virtual void Write(array<System::Byte>^ buffer, int32_t offset, int32_t count) override
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

                if (count > _bytesInBuffer - _positionInBuffer)
                {
                    throw gcnew System::IO::IOException("Insufficient space to write this number of bytes: " + count);
                }

                for (auto p = _buffer + _positionInBuffer; p < _buffer + _positionInBuffer + count; p++, offset++)
                {
                    *p = buffer[offset];
                }

                _positionInBuffer += count;
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
                int64_t get() override { return _bytesInBuffer; }
            }

            /// <summary>
            /// Not supported.
            /// </summary>
            property virtual int64_t Position
            {
                int64_t get() override { return _positionInBuffer; }
                void set(int64_t pos) override { Seek(pos, System::IO::SeekOrigin::Begin); }
            }
        };
    }
}