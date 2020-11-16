package fr.baldir.craft.standardizeazureservicebus.integration;

import com.microsoft.azure.servicebus.*;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

@Configuration
public class QueueConfiguration {


    private final String azureServiceBusConnectionString;

    public QueueConfiguration(
            @Value("${baldir.craft.azure.servicebus.connection-string}")
                    String azureServiceBusConnectionString) {
        this.azureServiceBusConnectionString = azureServiceBusConnectionString;
    }

    @Bean
    public IQueueClient sampleQueueClient(@Value("${baldir.craft.queue.sample.name}")
                                                  String queueName) throws Exception {
        // Connect to service bus
        QueueClient sampleQueueClient = new QueueClient(
                new ConnectionStringBuilder(azureServiceBusConnectionString, queueName),
                ReceiveMode.RECEIVEANDDELETE);

        initReceiver(sampleQueueClient);

        return sampleQueueClient;
    }


    void initReceiver(QueueClient queueClient) throws Exception {

        queueClient.registerMessageHandler(
                systemOutSampleMessageHandler(),
                new MessageHandlerOptions(1, true, Duration.ofMinutes(1)),
                ForkJoinPool.commonPool()); // Make explicit use of the default ServiceExecutor instead of using a deprecated methos
    }

    /**
     * @return Handler that prints the received message received from the queue.
     */
    private IMessageHandler systemOutSampleMessageHandler() {
        return new IMessageHandler() {
            public CompletableFuture<Void> onMessageAsync(IMessage message) {

                //{"this": "is a test"}
                System.out.printf("Message received : %s\n", new String(message.getBody()));

                return CompletableFuture.completedFuture(null);
            }

            public void notifyException(Throwable throwable, ExceptionPhase exceptionPhase) {
                System.out.printf(exceptionPhase + "-" + throwable.getMessage());
            }
        };
    }

}
