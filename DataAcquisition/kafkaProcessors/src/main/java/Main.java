public class Main {

    public static void main(String[] args) {
        RainingTriggerKafkaProcessor processor = new RainingTriggerKafkaProcessor();
        processor.process();
        System.out.println("Kafka Streams application finished.");
    }
}