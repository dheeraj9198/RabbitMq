import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.UUID;

/**
 * Created by dheeraj on 26/4/16.
 */
public class Main {

    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) throws Exception {

        new Thread(new Runnable() {
            public void run() {
                try {
                    ConnectionFactory factory = new ConnectionFactory();
                    factory.setHost("localhost");
                    Connection connection = factory.newConnection();
                    Channel channel = connection.createChannel();

                    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
                    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");


                    Consumer consumer = new DefaultConsumer(channel) {
                        @Override
                        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                                throws IOException {
                            String message = new String(body, "UTF-8");
                            System.out.println(" [x] Received '" + message + "'");
                        }
                    };
                    channel.basicConsume(QUEUE_NAME, true, consumer);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();

        while (true) {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            String message = "Hello World!"+ UUID.randomUUID().toString();
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");
            channel.close();
            connection.close();

            Thread.sleep(2000);

        }
    }

}
