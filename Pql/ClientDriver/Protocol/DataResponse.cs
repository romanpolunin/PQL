using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using ProtoBuf;

namespace Pql.ClientDriver.Protocol
{
    /// <summary>
    /// Data server's response schema. Gets written into the stream, then comes data stream.
    /// </summary>
    [ProtoContract]
    public class DataResponse
    {
        /// <summary>
        /// Error code. Zero for success.
        /// </summary>
        [ProtoMember(1, IsRequired = true)]
        public int ErrorCode;

        /// <summary>
        /// Detailed error information.
        /// </summary>
        [ProtoMember(2, IsRequired = false)]
        public string ServerMessage;

        /// <summary>
        /// Field metadata. Should be null if <see cref="ErrorCode"/> is non-zero.
        /// </summary>
        [ProtoMember(3, IsRequired = false)]
        public DataResponseField[] Fields;

        /// <summary>
        /// Number of records affected by non-query command.
        /// </summary>
        [ProtoMember(4, IsRequired = false)]
        public int RecordsAffected;
        
        /// <summary>
        /// Number of fields.
        /// </summary>
        public int FieldCount { get { return Fields == null ? 0 : Fields.Length; } }
        
        /// <summary>
        /// Ctr.
        /// </summary>
        public DataResponse()
        {}

        /// <summary>
        /// Ctr.
        /// </summary>
        public DataResponse(int errorCode, string serverMessage)
        {
            if (errorCode != 0 && string.IsNullOrEmpty(serverMessage))
            {
                throw new ArgumentNullException(serverMessage);
            }

            ErrorCode = errorCode;
            ServerMessage = serverMessage;
        }

        /// <summary>
        /// Ctr.
        /// </summary>
        /// <param name="fields">Field metadata</param>
        /// <exception cref="ArgumentNullException">Fields data is null</exception>
        public DataResponse(DataResponseField[] fields)
        {
            if (fields == null)
            {
                throw new ArgumentNullException("fields");
            }

            Fields = fields;
        }

        /// <summary>
        /// Looks up a field object by its internal name.
        /// </summary>
        /// <param name="name">Name to look for</param>
        /// <returns>Field metadata object</returns>
        /// <exception cref="IndexOutOfRangeException">Unknown name</exception>
        /// <exception cref="ArgumentNullException">Name is null</exception>
        public DataResponseField RequireField(string name)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException("name");
            }

            DataResponseField field;
            if (!m_fieldsByName.TryGetValue(name, out field))
            {
                throw new IndexOutOfRangeException("Unknown field: " + name);
            }

            return m_fieldsByName[name];
        }

        /// <summary>
        /// Looks up a field object by its index in the fields metadata.
        /// </summary>
        /// <param name="indexInResponse">Index of the field in the metadata</param>
        /// <returns>Field metadata object</returns>
        /// <exception cref="IndexOutOfRangeException">Invalid field index</exception>
        public DataResponseField RequireField(int indexInResponse)
        {
            if (indexInResponse < 0 || indexInResponse >= Fields.Length)
            {
                throw new IndexOutOfRangeException("Invalid field index: " + indexInResponse);
            }

            return Fields[indexInResponse];
        }

        /// <summary>
        /// Looks up a field name by its index in the fields metadata.
        /// </summary>
        /// <param name="indexInResponse">Index of the field in the metadata</param>
        /// <returns>Internal name of the field</returns>
        public string GetName(int indexInResponse)
        {
            return RequireField(indexInResponse).Name;
        }

        /// <summary>
        /// Reassembles the field-by-name lookup dictionary after deserialization.
        /// </summary>
        /// <param name="context"></param>
        [OnDeserialized]
        protected void AfterDeserialization(StreamingContext context)
        {
            if (Fields == null && string.IsNullOrEmpty(ServerMessage))
            {
                throw new InvalidOperationException("Fields or error message must be initialized");
            }

            if (Fields != null)
            {
                m_fieldsByName = new Dictionary<string, DataResponseField>();
                var ordinal = 0;
                foreach (var field in Fields)
                {
                    if (field == null)
                    {
                        throw new InvalidOperationException("Field is null as position " + ordinal);
                    }

                    if (ordinal != field.Ordinal)
                    {
                        throw new InvalidOperationException(string.Format("Field {0} is listed out of order. Found at {1}, expected at {2}", field.Name, ordinal, field.Ordinal));
                    }

                    while (m_fieldsByName.ContainsKey(field.Name))
                    {
                        field.Name = string.Format("{0}({1})", field.Name, ordinal);
                    }

                    m_fieldsByName.Add(field.Name, field);
                    ordinal++;
                }
            }
        }

        private Dictionary<string, DataResponseField> m_fieldsByName;
    }
}