using System;

namespace Pql.ClientDriver
{
    internal sealed class ConnectionProps
    {
        private readonly string m_protocolVersion;

        public readonly string RawString;
        public readonly Uri EndpointAddress;
        public readonly string ScopeId;
        public readonly int ConnectionTimeoutSeconds;

        public string Database
        {
            get { return ScopeId; }
        }

        public string ProtocolVersion
        {
            get { return m_protocolVersion; }
        }

        public ConnectionProps(string connectionString)
        {
            m_protocolVersion = "default";

            RawString = connectionString;
            
            if (string.IsNullOrEmpty(connectionString))
            {
                EndpointAddress = null;
                ScopeId = null;
                ConnectionTimeoutSeconds = 15;
                return;
            }

            var parts = connectionString.Split(';');
            foreach (var part in parts)
            {
                if (string.IsNullOrEmpty(part))
                {
                    continue;
                }

                var pair = part.Split('=');
                if (pair.Length != 2 || string.IsNullOrWhiteSpace(pair[0]))
                {
                    throw new ArgumentException("Invalid connection string part: " + part);
                }

                pair[0] = pair[0].Trim();
                if (pair[1] != null)
                {
                    pair[1] = pair[1].Trim();
                }

                if (0 == StringComparer.OrdinalIgnoreCase.Compare(pair[0], "Data Source")
                    || 0 == StringComparer.OrdinalIgnoreCase.Compare(pair[0], "Server"))
                {
                    EndpointAddress = new Uri(string.Concat("net.tcp://", pair[1]));
                }
                else if (0 == StringComparer.OrdinalIgnoreCase.Compare(pair[0], "Connect Timeout")
                         || 0 == StringComparer.OrdinalIgnoreCase.Compare(pair[0], "Connection Timeout"))
                {
                    if (!int.TryParse(pair[1], out ConnectionTimeoutSeconds) || ConnectionTimeoutSeconds < -1)
                    {
                        throw new ArgumentException("Invalid value for connection string property " + pair[0]);
                    }
                }
                else if (0 == StringComparer.OrdinalIgnoreCase.Compare(pair[0], "Initial Catalog")
                         || 0 == StringComparer.OrdinalIgnoreCase.Compare(pair[0], "Database"))
                {
                    ScopeId = pair[1] == null ? null : pair[1].Trim();
                }
                else
                {
                    throw new ArgumentException("Unknown connection string property name: " + part[0]);
                }
            }

            if (EndpointAddress == null)
            {
                throw new ArgumentException(@"Connection string must contain value for {Data Source}");
            }
        }
    }
}