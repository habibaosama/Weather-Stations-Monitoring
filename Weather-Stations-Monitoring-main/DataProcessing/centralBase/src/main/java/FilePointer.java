package org.example;

public class FilePointer {
    private final long fileId;
    private final long ValueSize;
    private final long ValueOffset;
    private final long timestamp;

    public FilePointer(long fileId, long valueSize, long valueOffset, long timestamp) {
        this.fileId = fileId;
        ValueSize = valueSize;
        ValueOffset = valueOffset;
        this.timestamp = timestamp;
    }

    public long getFileId() {
        return fileId;
    }

    public long getValueSize() {
        return ValueSize;
    }

    public long getValueOffset() {
        return ValueOffset;
    }
}
