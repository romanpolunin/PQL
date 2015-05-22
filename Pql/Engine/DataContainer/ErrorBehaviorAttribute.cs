using System;
using System.ServiceModel;
using System.ServiceModel.Channels;
using System.ServiceModel.Description;
using System.ServiceModel.Dispatcher;
using Pql.Engine.Interfaces;
using Pql.IntegrationStubs;

namespace Pql.Engine.DataContainer
{
    public class ErrorBehaviorAttribute : Attribute, IServiceBehavior
    {
        private readonly Type m_errorHandlerType;
        private readonly ITracer m_tracer;

        public ErrorBehaviorAttribute(Type errorHandlerType)
        {
            m_errorHandlerType = errorHandlerType;
            m_tracer = new DummyTracer();
        }

        void IServiceBehavior.Validate(ServiceDescription description, ServiceHostBase serviceHostBase)
        {
        }

        void IServiceBehavior.AddBindingParameters(ServiceDescription description, ServiceHostBase serviceHostBase, System.Collections.ObjectModel.Collection<ServiceEndpoint> endpoints, BindingParameterCollection parameters)
        {
        }

        void IServiceBehavior.ApplyDispatchBehavior(ServiceDescription description, ServiceHostBase serviceHostBase)
        {
            IErrorHandler errorHandler;

            try
            {
                errorHandler = (IErrorHandler)Activator.CreateInstance(m_errorHandlerType, m_tracer);
            }
            catch (MissingMethodException e)
            {
                throw new ArgumentException(string.Format(
                    "The errorHandlerType {0} must have a public constructor with single argument of type {1}",
                    m_errorHandlerType.AssemblyQualifiedName, typeof(ITracer).AssemblyQualifiedName)
                    , e);
            }
            catch (InvalidCastException e)
            {
                throw new ArgumentException(string.Format(
                    "The errorHandlerType {0} must implement System.ServiceModel.Dispatcher.IErrorHandler.",
                    m_errorHandlerType.AssemblyQualifiedName), e);
            }

            foreach (var channelDispatcherBase in serviceHostBase.ChannelDispatchers)
            {
                var channelDispatcher = channelDispatcherBase as ChannelDispatcher;
                if (channelDispatcher != null)
                {
                    channelDispatcher.ErrorHandlers.Add(errorHandler);
                }
            }
        }
    }
}