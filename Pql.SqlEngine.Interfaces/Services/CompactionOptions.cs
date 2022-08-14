namespace Pql.SqlEngine.Interfaces.Services
{
    [Flags]
    public enum CompactionOptions
    {
        /// <summary>
        /// Default option, does everything. Blocks all access until completed.
        /// </summary>
        FullRebuild = 0,
        /// <summary>
        /// Purges deleted records. Blocks all access until completed.
        /// </summary>
        PurgeDeleted = 1,
        /// <summary>
        /// Rebuilds dirty indexes. Blocks selects/updates/deletes that retrieve base dataset using affected indexes.
        /// </summary>
        FullReindex = 2
    }
}