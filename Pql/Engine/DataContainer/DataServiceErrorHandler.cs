using System;
using System.ServiceModel.Channels;
using System.ServiceModel.Dispatcher;
using Pql.Engine.Interfaces;

namespace Pql.Engine.DataContainer
{
    public class DataServiceErrorHandler : IErrorHandler 
    {
        private readonly ITracer m_tracer;

        public DataServiceErrorHandler(ITracer tracer)
        {
            if (tracer == null)
            {
                throw new ArgumentNullException("tracer");
            }    
            m_tracer = tracer;
        }

        public void ProvideFault(Exception error, MessageVersion version, ref Message fault)
        {
            //m_tracer.Exception(error);
        }

        public bool HandleError(Exception error)
        {
            m_tracer.Exception(error);
            return true;
        }
    }
}