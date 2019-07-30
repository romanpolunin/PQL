using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Irony.Parsing;

namespace Pql.Engine.Interfaces.Internal
{
    public class QueryPreprocessor
    {
        private readonly DataContainerDescriptor m_containerDescriptor;
        private readonly ConcurrentDictionary<string, ParseTreeNode> m_identifierAliasParts;

        public QueryPreprocessor(DataContainerDescriptor containerDescriptor)
        {
            m_containerDescriptor = containerDescriptor ?? throw new ArgumentNullException("containerDescriptor");
            m_identifierAliasParts = new ConcurrentDictionary<string, ParseTreeNode>(StringComparer.OrdinalIgnoreCase);
        }

        public void ProcessIdentifierAliases(ParseTreeNode root, DocumentTypeDescriptor targetEntity)
        {
            var ctx = new TreeIteratorContext
                {
                    Buffer = new List<string>(5),
                    TargetEntity = targetEntity,
                    CachedParts = m_identifierAliasParts
                };

            IterateTree(root, 0, ctx);
        }

        private static bool ProcessIdentifierAliasesFunctor(TreeIteratorContext ctx, ParseTreeNode node)
        {
            if (0 != StringComparer.Ordinal.Compare(node.Term.Name, "Id"))
            {
                return true;
            }

            ctx.Buffer.Clear();
            foreach (var part in node.ChildNodes)
            {
                ctx.Buffer.Add(part.Token.ValueString);
            }

            // first, try to find the alias of maximum length, 
            // then keep trimming tail until buffer is empty or we find some alias
            while (ctx.Buffer.Count > 0)
            {
                if (ctx.TargetEntity.TryGetIdentifierAlias(ctx.Buffer, out var mapped))
                {
                    // replace content of this identifier with pre-cached parts
                    node.ChildNodes.Clear();
                    foreach (var part in mapped)
                    {
                        node.ChildNodes.Add(GetCachedIdentifierAliasPart(part, ctx.CachedParts));
                    }

                    break;
                }

                ctx.Buffer.RemoveAt(ctx.Buffer.Count - 1);
            }

            return false;
        }

        private static ParseTreeNode GetCachedIdentifierAliasPart(string part, ConcurrentDictionary<string, ParseTreeNode> identifierAliasParts)
        {
            if (identifierAliasParts.TryGetValue(part, out var result))
            {
                return result;
            }

            result = new ParseTreeNode(new Token(new Terminal("id_simple"), new SourceLocation(), part, part));
            return identifierAliasParts.GetOrAdd(part, result);
        }

        private class TreeIteratorContext
        {
            public DocumentTypeDescriptor TargetEntity;
            public List<string> Buffer;
            public ConcurrentDictionary<string, ParseTreeNode> CachedParts;
        }

        private static void IterateTree(ParseTreeNode root, int level, TreeIteratorContext ctx)
        {
            if (!ProcessIdentifierAliasesFunctor(ctx, root))
            {
                // no need to go deeper into the tree
                return;
            }

            foreach (var child in root.ChildNodes)
            {
                IterateTree(child, level + 1, ctx);
            }
        }
    }
}
