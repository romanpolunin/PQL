using System;
using System.IO;
using System.Text;
using System.Threading;

namespace Pql.Engine.Interfaces.Internal
{
    /// <summary>
    /// A reusable pair of <see cref="MemoryStream"/> and <see cref="BinaryWriter"/>.
    /// MemoryStream is of fixed maximum capacity, based on a pre-allocated array (see <see cref="MaxBytesPerBuffer"/>).
    /// </summary>
    public sealed class RequestExecutionBuffer : IDisposable
    {
        private MemoryStream m_stream;
        private BinaryWriter m_writer;

        /// <summary>
        /// Maximum number of bytes that can be held in this buffer.
        /// Small value, because for now single process will serve multiple workspaces.
        /// Optimal value varies between 5Mb and 100Mb depending on how fast the data link to PQL client is.
        /// </summary>
        public const int MaxBytesPerBuffer = 5*1000*1000; 
        
        /// <summary>
        /// Number of rows (not bytes) written into this buffer.
        /// </summary>
        public long RowsOutput;

        /// <summary>
        /// An error that occured while writing last batch of rows into this buffer.
        /// </summary>
        public Exception Error;

        /// <summary>
        /// Pre-allocated stream to hold data.
        /// This stream is of fixed maximum capacity, based on a pre-allocated array (see <see cref="MaxBytesPerBuffer"/>).
        /// </summary>
        public MemoryStream Stream { get { CheckInitialized(); return m_stream; } }
        
        /// <summary>
        /// Pre-allocated writer, pointed to <see cref="Stream"/>.
        /// </summary>
        public BinaryWriter Writer { get { CheckInitialized(); return m_writer; } }

        /// <summary>
        /// Indicates whether writing to this buffer was not completed successfully.
        /// The data in <see cref="Stream"/> (if any) is still considered valid though, and may contain error information.
        /// Returns true when <see cref="Error"/> is not null.
        /// </summary>
        public bool IsFailed
        {
            get { return Error != null; }
        }

        /// <summary>
        /// Ctr.
        /// </summary>
        private void CheckInitialized()
        {
            if (m_stream == null)
            {
                m_stream = new MemoryStream(new byte[MaxBytesPerBuffer], 0, MaxBytesPerBuffer, true, true);
                
                // make sure length is zero, by default it will be set to length of underlying data buffer
                m_stream.SetLength(0);

                m_writer = new BinaryWriter(m_stream, Encoding.UTF8, true);
            }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
            Error = null;

            var writer = Interlocked.CompareExchange(ref m_writer, null, m_writer);
            if (writer != null)
            {
                writer.Dispose();
            }

            var stream = Interlocked.CompareExchange(ref m_stream, null, m_stream);
            if (stream != null)
            {
                stream.Dispose();
            }
        }

        /// <summary>
        /// Resets content of this buffer.
        /// </summary>
        public void Cleanup()
        {
            Error = null;
            RowsOutput = 0;

            var stream = m_stream;
            if (stream != null)
            {
                stream.SetLength(0);
            }
        }
    }
}