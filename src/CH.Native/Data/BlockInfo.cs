using CH.Native.Protocol;

namespace CH.Native.Data;

/// <summary>
/// Metadata for a data block including overflow and bucket information.
/// </summary>
public readonly struct BlockInfo
{
    /// <summary>
    /// Gets whether this block is an overflow block (for GROUP BY with TOTALS).
    /// </summary>
    public bool IsOverflows { get; init; }

    /// <summary>
    /// Gets the bucket number for distributed processing (-1 if not used).
    /// </summary>
    public int BucketNum { get; init; }

    /// <summary>
    /// Default BlockInfo for regular data blocks.
    /// </summary>
    public static BlockInfo Default { get; } = new BlockInfo
    {
        IsOverflows = false,
        BucketNum = -1
    };

    /// <summary>
    /// Reads BlockInfo from the protocol reader.
    /// </summary>
    /// <param name="reader">The protocol reader.</param>
    /// <returns>The parsed BlockInfo.</returns>
    public static BlockInfo Read(ref ProtocolReader reader)
    {
        bool isOverflows = false;
        int bucketNum = -1;

        while (true)
        {
            var fieldNum = reader.ReadVarInt();
            if (fieldNum == 0)
                break;

            switch (fieldNum)
            {
                case 1:
                    isOverflows = reader.ReadByte() != 0;
                    break;
                case 2:
                    bucketNum = reader.ReadInt32();
                    break;
                default:
                    // Unknown field, skip
                    break;
            }
        }

        return new BlockInfo
        {
            IsOverflows = isOverflows,
            BucketNum = bucketNum
        };
    }

    /// <summary>
    /// Writes BlockInfo to the protocol writer.
    /// </summary>
    /// <param name="writer">The protocol writer.</param>
    public void Write(ref ProtocolWriter writer)
    {
        // Field 1: IsOverflows
        writer.WriteVarInt(1);
        writer.WriteByte(IsOverflows ? (byte)1 : (byte)0);

        // Field 2: BucketNum
        writer.WriteVarInt(2);
        writer.WriteInt32(BucketNum);

        // End marker
        writer.WriteVarInt(0);
    }
}
