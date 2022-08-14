#pragma once

namespace Pql 
{
	namespace UnmanagedLib 
	{
		[System::SerializableAttribute()]
		public ref class MemoryManagerException : System::Exception
		{
		public:
			MemoryManagerException()
			{}

			MemoryManagerException(System::String^ message) : Exception(message)
			{}

			MemoryManagerException(System::String^ message, Exception^ inner) : Exception(message, inner)
			{
			}

			MemoryManagerException(
				System::Runtime::Serialization::SerializationInfo^ info,
				System::Runtime::Serialization::StreamingContext context) : Exception(info, context)
			{}
		};
	}
}