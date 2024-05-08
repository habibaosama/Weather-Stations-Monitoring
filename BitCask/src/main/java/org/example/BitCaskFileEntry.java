package org.example;


public class BitCaskFileEntry {


    private final int keySize;
    private final long valueSize;
    private final int key;
    private final byte[] value;

    public BitCaskFileEntry(int keySize, long valueSize, int key, byte[] value) {
        this.keySize = keySize;
        this.valueSize = valueSize;
        this.key = key;
        this.value = value;
    }

    public int getKeySize() {
        return keySize;
    }

    public long getValueSize() {
        return valueSize;
    }

    public int getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }


}