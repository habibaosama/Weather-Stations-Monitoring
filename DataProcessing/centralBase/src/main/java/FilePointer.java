package org.example;

/**
 * This class represents a DataPointer with a fileId, valueSize, valueOffset, and timestamp.
 */
public class FilePointer {
    private final long fileId;
    private final long valueSize;
    private final long valueOffset;
    private final long timestamp;

    /**
     * Constructs a new DataPointer.
     *
     * @param fileId      the id of the file
     * @param valueSize   the size of the value
     * @param valueOffset the offset of the value
     * @param timestamp   the timestamp of the data
     */
    public FilePointer(long fileId, long valueSize, long valueOffset, long timestamp) {
        this.fileId = fileId;
        this.valueSize = valueSize;
        this.valueOffset = valueOffset;
        this.timestamp = timestamp;
    }

    /**
     * Returns the id of the file.
     *
     * @return the file id
     */
    public long getFileId() {
        return fileId;
    }

    /**
     * Returns the size of the value.
     *
     * @return the value size
     */
    public long getValueSize() {
        return valueSize;
    }

    /**
     * Returns the offset of the value.
     *
     * @return the value offset
     */
    public long getValueOffset() {
        return valueOffset;
    }

    /**
     * Returns the timestamp of the data.
     *
     * @return the timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }
}