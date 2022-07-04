using System;
using Pql.ClientDriver.Protocol;

namespace Pql.Engine.Interfaces.Internal
{
    public class RequestExecutionContextCacheInfo
    {
        public readonly long HashCode;
        public readonly ParsedRequest ParsedRequest;
        public readonly DataRequestParams RequestParams;
        public readonly DataRequestBulk RequestBulk;
        public string CommandText;
        
        public volatile Exception ErrorInfo;
        public volatile bool HaveRequestHeaders;
        public volatile bool HaveParsingResults;

        public RequestExecutionContextCacheInfo(long hashCode)
        {
            if (hashCode == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(hashCode), hashCode, "hashCode must be non-zero");
            }

            HashCode = hashCode;
            ParsedRequest = new ParsedRequest(true);
            RequestParams = new DataRequestParams();
            RequestBulk = new DataRequestBulk();
        }

        /// <summary>
        /// Reads portion of request headers appropriate for caching into the cache buffer.
        /// </summary>
        public void ReadRequestHeaders(DataRequest request, DataRequestParams requestParams, DataRequestBulk requestBulk, ParsedRequest parsedRequest)
        {
            if (request == null)
            {
                throw new ArgumentNullException(nameof(requestParams));
            }

            CommandText = request.CommandText;
            
            RequestParams.Clear();
            RequestBulk.Clear();

            if (request.HaveParameters)
            {
                RequestParams.DataTypes = requestParams.DataTypes;
                RequestParams.IsCollectionFlags = requestParams.IsCollectionFlags;
                RequestParams.Names = requestParams.Names;

                // some values are written into parsed request during request read process
                // have to replicate them in the cached version, because they are required for subsequent compilation
                ParsedRequest.Params.Names = parsedRequest.Params.Names;
                ParsedRequest.Params.DataTypes = parsedRequest.Params.DataTypes;
                ParsedRequest.Params.OrdinalToLocalOrdinal = parsedRequest.Params.OrdinalToLocalOrdinal;
            }

            if (request.HaveRequestBulk)
            {
                RequestBulk.DbStatementType = requestBulk.DbStatementType;
                RequestBulk.EntityName = requestBulk.EntityName;
                RequestBulk.FieldNames = requestBulk.FieldNames;

                // some values are written into parsed request during request read process
                // have to replicate them in the cached version, because they are required for subsequent compilation
                ParsedRequest.IsBulk = parsedRequest.IsBulk;
            }
            
            if (StringComparer.OrdinalIgnoreCase.Equals(CommandText, "defragment"))
            {
                ParsedRequest.SpecialCommand.IsSpecialCommand = true;
                ParsedRequest.SpecialCommand.CommandType = ParsedRequest.SpecialCommandData.SpecialCommandType.Defragment;
            }

            HaveRequestHeaders = true;
        }

        /// <summary>
        /// Writes cacheable portion of request parsing and compilation information into the request context.
        /// </summary>
        public void WriteParsingResults(ParsedRequest parsedRequest)
        {
            ParsedRequest.BaseDataset.WriteTo(ref parsedRequest.BaseDataset);
            ParsedRequest.Select.WriteTo(ref parsedRequest.Select);
            ParsedRequest.Modify.WriteTo(ref parsedRequest.Modify);
            
            parsedRequest.OrdinalOfPrimaryKey = ParsedRequest.OrdinalOfPrimaryKey;
            parsedRequest.TargetEntity = ParsedRequest.TargetEntity;
            parsedRequest.TargetEntityPkField = ParsedRequest.TargetEntityPkField;
            parsedRequest.StatementType = ParsedRequest.StatementType;

            parsedRequest.SpecialCommand.IsSpecialCommand = ParsedRequest.SpecialCommand.IsSpecialCommand;
            parsedRequest.SpecialCommand.CommandType = ParsedRequest.SpecialCommand.CommandType;
        }

        public void CheckIsError()
        {
            var errorInfo = ErrorInfo;
            if (errorInfo != null)
            {
                throw new Exception(errorInfo.Message, errorInfo);
            }
        }
        
        public void IsError(Exception exception)
        {
            ErrorInfo = exception;
            HaveParsingResults = false;
            HaveRequestHeaders = false;
            ParsedRequest.Clear();
            RequestBulk.Clear();
            RequestParams.Clear();
        }
    }
}