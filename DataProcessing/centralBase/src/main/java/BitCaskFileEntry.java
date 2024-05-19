package org.example;

/**
 * This class represents a BitCask file entry.
 */
public class BitCaskFileEntry {
    private final long timestamp;
    private final int keySize;
    private final long valueSize;
    private final int key;
    private final byte[] value;

    /**
     * Constructs a new BitCaskFileEntry.
     *
     * @param timestamp the timestamp of the entry
     * @param keySize the size of the key
     * @param valueSize the size of the value
     * @param key the key of the entry
     * @param value the value of the entry
     */
    public BitCaskFileEntry(long timestamp, int keySize, long valueSize, int key, byte[] value) {
        this.timestamp = timestamp;
        this.keySize = keySize;
        this.valueSize = valueSize;
        this.key = key;
        this.value = value;
    }

    // Getters

    /**
     * Returns the timestamp of the entry.
     *
     * @return the timestamp
     */
    public long getTimestamp() {
        return timestamp;
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
     * Returns the key of the entry.
     *
     * @return the key
     */
    public int getKey() {
        return key;
    }

    /**
     * Returns the value of the entry.
     *
     * @return the value
     */
    public byte[] getValue() {
        return value;
    }
}