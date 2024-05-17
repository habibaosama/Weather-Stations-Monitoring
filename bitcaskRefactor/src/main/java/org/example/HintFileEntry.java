package org.example;

/**
 * This class represents a HintFileEntry with a timestamp, key, keySize, valueSize, and valueOffset.
 */
public class HintFileEntry {
    private final long timestamp;
    private final int key;
    private final int keySize;
    private final long valueSize;
    private final long valueOffset;

    /**
     * Constructs a new HintFileEntry.
     *
     * @param timestamp the timestamp of the entry
     * @param key the key of the entry
     * @param keySize the size of the key
     * @param valueSize the size of the value
     * @param valueOffset the offset of the value
     */
    public HintFileEntry(long timestamp, int key, int keySize, long valueSize, long valueOffset) {
        this.timestamp = timestamp;
        this.key = key;
        this.keySize = keySize;
        this.valueSize = valueSize;
        this.valueOffset = valueOffset;
    }

    /**
     * Returns the key of the entry.
     *
     * @return the key
     */
    public int getKey() {
        return key;
    }

    /**
     * Returns the size of the key.
     *
     * @return the key size
     */
    public int getKeySize() {
        return keySize;
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
     * Returns the timestamp of the entry.
     *
     * @return the timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }
}