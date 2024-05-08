package org.example;

public class HintFileEntry {
    private final int key;
    private final int keySize;
    private final long ValueSize;
    private final long ValueOffset;

    public HintFileEntry(int key, int keySize, long valueSize, long valueOffset) {
        this.key = key;
        this.keySize = keySize;
        ValueSize = valueSize;
        ValueOffset = valueOffset;
    }

    public int getKey() {
        return key;
    }

    public int getKeySize() {
        return keySize;
    }

    public long getValueSize() {
        return ValueSize;
    }

    public long getValueOffset() {
        return ValueOffset;
    }


}
