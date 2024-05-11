//public class Main {
//    public static void main(String[] args) throws Exception {
//        BaseCentralStation.consume();
//    }
//}

public class Main {
    public static void main(String[] args) throws Exception {
        BitCask bitCask = new BitCask();
     //   bitCask.open("src/main/java/test");
        BaseCentralStation.consume();
//        for (int i = 0; i < 10; i++) {
//            System.out.println(bitCask.get(i));
//        }


    }

}