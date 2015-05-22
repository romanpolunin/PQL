using System;
using System.IO;
using System.Text;

namespace Pql.ClientDriver
{
    /// <summary>
    /// A reusable buffering reader. Can be attached to multiple streams at different times.
    /// Implements block-oriented streaming transfer protocol, with markers.
    /// </summary>
    public sealed class BufferedReaderStream : Stream
    {
        private readonly byte[] m_buffer;
        private Stream m_source;
        private int m_yetUnreadBytesInBlock;
        private int m_positionInBuffer;
        private int m_bytesInBuffer;
        private readonly byte[] m_markerBuffer; 
        private readonly BinaryReader m_myReader;
        private BinaryReader m_sourceReader;
        private bool m_completed;

        /// <summary>
        /// Ctr.
        /// </summary>
        /// <param name="bufferSize">Size of the buffer to allocate</param>
        public BufferedReaderStream(int bufferSize)
        {
            if (bufferSize <= 0)
            {
                throw new ArgumentOutOfRangeException("bufferSize", bufferSize, "Invalid buffer size");
            }

            m_buffer = new byte[bufferSize];
            m_myReader = new BinaryReader(this, Encoding.UTF8, true);
            m_markerBuffer = new byte[1];
        }

        /// <summary>
        /// Ctr.
        /// </summary>
        /// <param name="stream">Stream to attach to, initially</param>
        /// <param name="bufferSize">Size of the buffer</param>
        public BufferedReaderStream(Stream stream, int bufferSize) : this(bufferSize)
        {
            Attach(stream);
        }

        /// <summary>
        /// Resets stream state and re-attaches it to another base stream.
        /// </summary>
        /// <param name="stream">Another base stream to attach to</param>
        public void Attach(Stream stream)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("stream");
            }

            if (!stream.CanRead)
            {
                throw new ArgumentException("Base stream must support reading", "stream");
            }

            Detach();
            
            m_source = stream;
            m_sourceReader = new BinaryReader(m_source, Encoding.UTF8, true);
        }

        /// <summary>
        /// Removes reference to the base stream.
        /// </summary>
        public void Detach()
        {
            m_positionInBuffer = 0;
            m_bytesInBuffer = 0;
            m_yetUnreadBytesInBlock = 0;
            m_completed = false;
            m_source = null;
            m_sourceReader = null;
        }

        /// <summary>
        /// Releases the unmanaged resources used by the <see cref="T:System.IO.Stream"/> and optionally releases the managed resources.
        /// </summary>
        /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
        protected override void Dispose(bool disposing)
        {
            Detach();
            base.Dispose(disposing);
        }

        /// <summary>
        /// Reader on this stream.
        /// </summary>
        public BinaryReader MyReader
        {
            get { return m_myReader; }
        }

        /// <summary>
        /// When overridden in a derived class, clears all buffers for this stream and causes any buffered data to be written to the underlying device.
        /// </summary>
        /// <exception cref="T:System.IO.IOException">An I/O error occurs. </exception><filterpriority>2</filterpriority>
        public override void Flush()
        {
            
        }

        /// <summary>
        /// Not supported.
        /// </summary>
        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Not supported.
        /// </summary>
        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// When overridden in a derived class, reads a sequence of bytes from the current stream and advances the position within the stream by the number of bytes read.
        /// </summary>
        /// <returns>
        /// The total number of bytes read into the buffer. This can be less than the number of bytes requested if that many bytes are not currently available, or zero (0) if the end of the stream has been reached.
        /// </returns>
        /// <param name="buffer">An array of bytes. When this method returns, the buffer contains the specified byte array with the values between <paramref name="offset"/> and (<paramref name="offset"/> + <paramref name="count"/> - 1) replaced by the bytes read from the current source. </param><param name="offset">The zero-based byte offset in <paramref name="buffer"/> at which to begin storing the data read from the current stream. </param><param name="count">The maximum number of bytes to be read from the current stream. </param><exception cref="T:System.ArgumentException">The sum of <paramref name="offset"/> and <paramref name="count"/> is larger than the buffer length. </exception><exception cref="T:System.ArgumentNullException"><paramref name="buffer"/> is null. </exception><exception cref="T:System.ArgumentOutOfRangeException"><paramref name="offset"/> or <paramref name="count"/> is negative. </exception><exception cref="T:System.IO.IOException">An I/O error occurs. </exception><exception cref="T:System.NotSupportedException">The stream does not support reading. </exception><exception cref="T:System.ObjectDisposedException">Methods were called after the stream was closed. </exception><filterpriority>1</filterpriority>
        public override int Read(byte[] buffer, int offset, int count)
        {
            if (m_completed)
            {
                return 0;
            }

            int bytesRead;
            if (m_bytesInBuffer == m_positionInBuffer)
            {
                m_positionInBuffer = 0;
                m_bytesInBuffer = 0;

                int toRead;
                if (m_yetUnreadBytesInBlock == 0)
                {
                    ReadMarker();
                    var marker = m_markerBuffer[0];
                    if (marker == RowData.StreamEnd[0])
                    {
                        MarkCompleted();
                        return 0;
                    }

                    if (marker != RowData.BlockHead[0])
                    {
                        MarkCompleted();
                        throw new Exception("Invalid block header marker: " + marker);
                    }

                    toRead = m_yetUnreadBytesInBlock = ReadBlockSize();
                }
                else
                {
                    toRead = m_yetUnreadBytesInBlock;
                }

                toRead = Math.Min(toRead, m_buffer.Length);
                while (toRead > 0)
                {
                    bytesRead = m_sourceReader.Read(m_buffer, m_bytesInBuffer, toRead);
                    if (bytesRead == 0)
                    {
                        MarkCompleted();
                        throw new Exception("Unexpected end of stream. Expected to have " + m_yetUnreadBytesInBlock + " more bytes");
                    }

                    m_bytesInBuffer += bytesRead;
                    toRead -= bytesRead;
                    m_yetUnreadBytesInBlock -= bytesRead;
                }

                if (toRead < 0)
                {
                    MarkCompleted();
                    throw new IOException("Remaining bytes to read cannot turn negative");
                }
            }

            bytesRead = Math.Min(count, m_bytesInBuffer - m_positionInBuffer);
            Buffer.BlockCopy(m_buffer, m_positionInBuffer, buffer, offset, bytesRead);

            m_positionInBuffer += bytesRead;
            return bytesRead;
        }

        private void ReadMarker()
        {
            if (1 != m_sourceReader.Read(m_markerBuffer, 0, 1))
            {
                throw new IOException("Could not read marker from input stream");
            }
        }

        private void MarkCompleted()
        {
            m_completed = true;
            m_sourceReader = null;
            m_source = null;
        }

        private int ReadBlockSize()
        {
            var result = m_sourceReader.ReadInt32();
            if (result <= 0 || result >= int.MaxValue)
            {
                throw new Exception("Invalid incoming block size: " + result);
            }

            return result;
        }

        /// <summary>
        /// Not supported.
        /// </summary>
        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Returns true.
        /// </summary>
        /// <returns>
        /// true if the stream supports reading; otherwise, false.
        /// </returns>
        /// <filterpriority>1</filterpriority>
        public override bool CanRead
        {
            get { return true; }
        }

        /// <summary>
        /// Returns false.
        /// </summary>
        /// <returns>
        /// true if the stream supports seeking; otherwise, false.
        /// </returns>
        /// <filterpriority>1</filterpriority>
        public override bool CanSeek
        {
            get { return false; }
        }

        /// <summary>
        /// Returns false.
        /// </summary>
        /// <returns>
        /// true if the stream supports writing; otherwise, false.
        /// </returns>
        /// <filterpriority>1</filterpriority>
        public override bool CanWrite
        {
            get { return false; }
        }

        /// <summary>
        /// Not supported.
        /// </summary>
        public override long Length
        {
            get { throw new NotSupportedException(); }
        }

        /// <summary>
        /// Not supported.
        /// </summary>
        public override long Position { get { throw new NotSupportedException(); } set { throw new NotSupportedException(); } }

        /// <summary>
        /// Protocol support.
        /// </summary>
        public static void WriteBlock(BinaryWriter writer, int toWrite, byte[] data)
        {
            // write block start marker. 
            // do NOT use BinaryWriter's and Stream's methods that write a single byte - write everything as arrays, even a single-element
            // WriteByte methods cause interference with WCF framing protocol
            writer.Write(RowData.BlockHead, 0, RowData.BlockHead.Length);

            // write block size
            writer.Write(toWrite);

            // writer
            writer.Write(data, 0, toWrite);
        }

        /// <summary>
        /// Protocol support.
        /// </summary>
        public static void WriteBlock(BinaryWriter writer, MemoryStream data)
        {
            var toWrite = checked((Int32) data.Length);
            if (toWrite > 0)
            {
                // write block start marker. 
                // do NOT use BinaryWriter's and Stream's methods that write a single byte - write everything as arrays, even a single-element
                // WriteByte methods cause interference with WCF framing protocol
                writer.Write(RowData.BlockHead, 0, RowData.BlockHead.Length);

                // write block size
                writer.Write(toWrite);

                // write data
                writer.Flush();
                data.Position = 0;
                data.CopyTo(writer.BaseStream);
            }
        }

        /// <summary>
        /// Protocol support.
        /// </summary>
        public static void WriteStreamEndMarker(BinaryWriter binaryWriter)
        {
            // write stream end marker for client
            binaryWriter.Write(RowData.StreamEnd);
        }
    }
}