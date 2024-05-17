package org.example;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        Bitcask bitCask = new Bitcask();
        bitCask.open("./data");
        for (int i = 0; i < 10000; i++) {
            bitCask.put(i % 10, "value" + i);
        }
    }
}