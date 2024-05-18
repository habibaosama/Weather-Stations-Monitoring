package org.example;


import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        BitCask bitCask = new BitCask();
        bitCask.open("./data");
        new Thread(() -> {
            for (int i = 0; i < 10000; i++) {
                try {
                    bitCask.put(i % 10, "value" + i);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                try {
                    System.out.println(bitCask.get(i % 10));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();

    }

}