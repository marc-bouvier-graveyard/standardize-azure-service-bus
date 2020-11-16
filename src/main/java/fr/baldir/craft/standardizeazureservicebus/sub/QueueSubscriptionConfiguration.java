package fr.baldir.craft.standardizeazureservicebus.sub;

import com.microsoft.azure.servicebus.*;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

@Configuration
public class QueueSubscriptionConfiguration {


    private final String azureServiceBusConnectionString;

    public QueueSubscriptionConfiguration(
            @Value("${spring.jms.servicebus.connection-string}")
                    String azureServiceBusConnectionString) {
        this.azureServiceBusConnectionString = azureServiceBusConnectionString;
    }

    @Bean
    public IQueueClient queueClient() throws Exception {
        // Connect to service bus
        QueueClient queueClient = new QueueClient(
                new ConnectionStringBuilder(azureServiceBusConnectionString, "sub-queue"),
                ReceiveMode.RECEIVEANDDELETE);
        registerReceiver(queueClient);

        return queueClient;
    }

    void registerReceiver(QueueClient queueClient) throws Exception {

        queueClient.registerMessageHandler(
                messageHandler(),
                new MessageHandlerOptions(1, true, Duration.ofMinutes(1)),
                ForkJoinPool.commonPool());
    }

    @NotNull
    private IMessageHandler messageHandler() {
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
