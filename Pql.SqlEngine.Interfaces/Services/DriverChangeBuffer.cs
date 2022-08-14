using Pql.SqlEngine.Interfaces.Internal;

namespace Pql.SqlEngine.Interfaces.Services
{
    /// <summary>
    /// Holds information about changed fields.
    /// For now, assumes that ALL fields are changed.
    /// </summary>
    public class DriverChangeBuffer
    {
        /// <summary>
        /// Handle to descriptor of entity being updated in current request.
        /// </summary>
        public readonly int TargetEntity;
        /// <summary>
        /// Fields metadata, same order as in <see cref="Data"/>.
        /// Not used for deletes.
        /// </summary>
        public readonly FieldMetadata[]? Fields;
        /// <summary>
        /// Values holder, same ordinals order as in <see cref="Fields"/>.
        /// Not used for deletes.
        /// </summary>
        public readonly DriverRowData? Data;

        /// <summary>
        /// Type of the change.
        /// </summary>
        public DriverChangeType ChangeType;

        /// <summary>
        /// Identifier of the entity to be updated or deleted when this buffer is passed to storage driver.
        /// </summary>
        public byte[]? InternalEntityId;

        /// <summary>
        /// Ordinal of the value of primary key inside <see cref="Data"/>.
        /// </summary>
        public readonly int OrdinalOfPrimaryKey;

        /// <summary>
        /// Ctr.
        /// </summary>
        public DriverChangeBuffer(int targetEntity)
        {
            TargetEntity = targetEntity;
            OrdinalOfPrimaryKey = -1;
        }
        
        /// <summary>
        /// Ctr.
        /// </summary>
        public DriverChangeBuffer(int targetEntity, int ordinalOfPrimaryKey, FieldMetadata[] fields)
        {
            Fields = fields;
            TargetEntity = targetEntity;
            Data = new DriverRowData(fields.Select(x => x.DbType).ToArray());
            OrdinalOfPrimaryKey = ordinalOfPrimaryKey;
        }
    }
}