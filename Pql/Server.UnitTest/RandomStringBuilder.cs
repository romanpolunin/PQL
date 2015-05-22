using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Pql.Server.UnitTest
{
    public class RandomStringBuilder
    {
        private readonly Random m_rand = new Random(0); 
        private readonly StringBuilder m_builder = new StringBuilder();
        private readonly string[] m_parts;
        private int m_lastId;

        public RandomStringBuilder()
        {
            using (var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream("Pql.Server.UnitTest.TextFile.txt"))
            {
                using (var reader = new StreamReader(stream))
                {
                    var blob = reader.ReadToEnd().Split(' ');
                    m_parts = blob.Where(x => x.Length >= 5).ToArray();
                }
            }
        }

        public string GenerateRandomString()
        {
            return "string" + ++m_lastId;

            var length = 1 + m_rand.Next(20);
            
            m_builder.Clear();
            while (m_builder.Length < length)
            {
                m_builder.Append(m_parts[m_rand.Next(m_parts.Length)]);
                m_builder.Append(' ');
            }

            return m_builder.ToString();
        }
    }
}