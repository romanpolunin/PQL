using System;
using System.IO;
using System.Text;
using Pql.ClientDriver;
using Pql.ClientDriver.Protocol;
using Pql.ClientDriver.Wcf;
using Pql.Engine.Interfaces;
using Pql.Engine.Interfaces.Internal;
using Pql.Engine.Interfaces.Services;

namespace Pql.Engine.DataContainer.Engine
{
    /// <summary>
    /// RequestProcessingManager's (RPM) <see cref="WriteTo"/> works in parallel with <see cref="DataEngine.ProducerThreadMethod"/>.
    /// RPM supplies empty buffers to be filled with data into <see cref="RequestExecutionContext.BuffersRing"/> and consumes them on the other end.
    /// The data ring has very limited number of buffers.
    /// RPM is limited by network throughput and Producer's speed.
    /// Producer is limited by underlying storage driver, local processing speed and RPM's consumption of complete buffers.
    /// The difference between the two: RPM is scheduled for execution by service infrastructure (WCF),
    /// whereas ProducerThreadMethod is scheduled by RPM itself, when it invokes <see cref="IDataEngine.BeginExecution"/>.
    /// </summary>
    internal class RequestProcessingManager : Stream, IPqlDataWriter
    {
        private readonly RequestExecutionContext m_executionContext;
        private readonly RawDataWriterPerfCounters m_counters;

        public RequestProcessingManager(ITracer tracer, IPqlEngineHostProcess process, RawDataWriterPerfCounters counters)
        {
            if (tracer == null)
            {
                throw new ArgumentNullException("tracer");
            }

            if (process == null)
            {
                throw new ArgumentNullException("process");
            }

            m_counters = counters ?? throw new ArgumentNullException("counters");
            m_executionContext = new RequestExecutionContext(process, tracer);
        }

        public void Attach(PqlMessage requestMessage, IDataEngine dataEngine, IPqlClientSecurityContext authContext)
        {
            if (dataEngine == null)
            {
                throw new ArgumentNullException("dataEngine");
            }

            if (authContext == null)
            {
                throw new ArgumentNullException("authContext");
            }

            m_executionContext.AssertIsClean();
            m_executionContext.AttachInputMessage(requestMessage, dataEngine, authContext);
        }

        public RequestExecutionContext ExecutionContext
        {
            get { return m_executionContext; }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                GC.SuppressFinalize(this);
            }

            m_executionContext.Dispose();

            base.Dispose(disposing);
        }

        public override void Flush()
        {
            
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

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
        /// <param name="buffer">An array of bytes. When this method returns, the buffer contains the specified byte array with the values between <paramref name="offset"/> 
        /// and (<paramref name="offset"/> + <paramref name="count"/> - 1) replaced by the bytes read from the current source. </param>
        /// <param name="offset">The zero-based byte offset in <paramref name="buffer"/> at which to begin storing the data read from the current stream. </param>
        /// <param name="count">The maximum number of bytes to be read from the current stream. </param>
        /// <exception cref="T:System.ArgumentException">The sum of <paramref name="offset"/> and <paramref name="count"/> is larger than the buffer length. </exception>
        /// <exception cref="T:System.ArgumentNullException"><paramref name="buffer"/> is null. </exception>
        /// <exception cref="T:System.ArgumentOutOfRangeException"><paramref name="offset"/> or <paramref name="count"/> is negative. </exception>
        /// <exception cref="T:System.IO.IOException">An I/O error occurs. </exception><exception cref="T:System.NotSupportedException">The stream does not support reading. </exception>
        /// <exception cref="T:System.ObjectDisposedException">Methods were called after the stream was closed. </exception><filterpriority>1</filterpriority>
        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// RequestProcessingManager's (RPM) <see cref="WriteTo"/> works in parallel with <see cref="DataEngine.ProducerThreadMethod"/>.
        /// RPM supplies empty buffers to be filled with data into <see cref="RequestExecutionContext.BuffersRing"/> and consumes them on the other end.
        /// The data ring has very limited number of buffers.
        /// RPM is limited by network throughput and Producer's speed.
        /// Producer is limited by underlying storage driver, local processing speed and RPM's consumption of complete buffers.
        /// The difference between the two: RPM <see cref="WriteTo"/> is scheduled for execution by service infrastructure (WCF),
        /// whereas <see cref="DataEngine.ProducerThreadMethod"/> is scheduled by RPM itself, when it invokes <see cref="IDataEngine.BeginExecution"/>.
        /// </summary>
        public void WriteTo(Stream output)
        {
            // We will not report to client any unhandled errors from producer thread.
            // As of our local errors, we can only send them to client BEFORE the first block of data is streamed out.
            var canReportLocalErrors = true;
            
            try
            {
                using (var binaryWriter = new BinaryWriter(output, Encoding.UTF8, true))
                {
                    // rotate through the buffers, flush them to output until producer stops
                    var cancellation = m_executionContext.CancellationTokenSource.Token;
                    var buffersRing = m_executionContext.BuffersRing;
                    var lastCompletedTask = buffersRing.TakeCompletedTask(cancellation);
                    while (lastCompletedTask != null)
                    {
                        // no more reporting of local errors after first block has been generated
                        // note that this block may itself contain error information, but this is unrelated
                        canReportLocalErrors = false;

                        var stream = lastCompletedTask.Stream;
                        var toWrite = checked((int) stream.Length);
                        if (toWrite == 0)
                        {
                            break;
                        }

                        // write block
                        var buffer = stream.GetBuffer();
                        BufferedReaderStream.WriteBlock(binaryWriter, toWrite, buffer);

                        ReportStats(toWrite, lastCompletedTask.RowsOutput);

                        // this task might have produced error information instead of real data
                        // or it might have finished producing rows
                        // in both cases, break further processing
                        if (lastCompletedTask.IsFailed || lastCompletedTask.RowsOutput == 0)
                        {
                            break;
                        }

                        // return buffer to the ring
                        buffersRing.AddTaskForProcessing(lastCompletedTask, cancellation);
                        // take next one
                        lastCompletedTask = buffersRing.TakeCompletedTask(cancellation);
                    }

                    BufferedReaderStream.WriteStreamEndMarker(binaryWriter);

                    // no more data in the input sequence, or just have to stop producing
                    buffersRing.CompleteAddingTasksForProcessing();
                }
            }
            catch (Exception e)
            {
                m_executionContext.Cancel(e);

                if (canReportLocalErrors)
                {
                    using (var errorWriter = new PqlErrorDataWriter(1, m_executionContext.LastError, true))
                    {
                        errorWriter.WriteTo(output);
                    }
                }

                if (!(e is OperationCanceledException))
                {
                    throw;
                }
            }
        }

        private void ReportStats(long bytesReturned, long rowsReturned)
        {
            m_counters.ByteRate.IncrementBy(bytesReturned);
            m_counters.RowRate.IncrementBy(rowsReturned);
            m_counters.TotalBytes.IncrementBy(bytesReturned);
            m_counters.TotalRows.IncrementBy(rowsReturned);
        }

        /// <summary>
        /// When overridden in a derived class, writes a sequence of bytes to the current stream and advances the current position within this stream by the number of bytes written.
        /// </summary>
        /// <param name="buffer">An array of bytes. This method copies <paramref name="count"/> bytes from <paramref name="buffer"/> to the current stream. </param>
        /// <param name="offset">The zero-based byte offset in <paramref name="buffer"/> at which to begin copying bytes to the current stream. </param>
        /// <param name="count">The number of bytes to be written to the current stream. </param><filterpriority>1</filterpriority>
        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return false; }
        }

        public override long Length
        {
            get { throw new NotSupportedException(); }
        }

        public override long Position
        {
            get { throw new NotSupportedException(); } set { throw new NotSupportedException(); }
        }
    }
}
