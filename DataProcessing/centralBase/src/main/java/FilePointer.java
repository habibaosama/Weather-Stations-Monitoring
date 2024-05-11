public class FilePointer {
    private final long fileId;
    private final long ValueSize;
    private final long ValueOffset;

    public FilePointer(long fileId, long valueSize, long valueOffset) {
        this.fileId = fileId;
        ValueSize = valueSize;
        ValueOffset = valueOffset;
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
