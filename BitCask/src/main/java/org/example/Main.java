package org.example;


public class Main {
    public static void main(String[] args) {
        BitCask bitCask = new BitCask();
        bitCask.open("./src/main/java/org/example/test");

        for (int i = 0; i < 10; i++) {
            System.out.println(bitCask.get(i));
        }


    }

}