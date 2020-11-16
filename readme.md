# Context
[Git repo of this project](https://github.com/marc-bouvier/standardize-azure-service-bus).

A legacy application uses Azure Sdk for AMQP queue message receiveing and sending.

We want to move away from Microsoft SDK to a more standard provider (AMQP 1.0 compatible).
E.G. to move to another cloud provider or on premise.

## Behaviou

A simple web application with an endpoint to send a message to a queue "sample-queue" 
- POST /send (message is the body)

The queue "sample-queue" is listened and outputs the received message in the standard output.

## 01 - using Azure Classic SDK

This is the starting context. We want to move away from it.
We are tightly coupled to Azure Platform & SDK

pom.xml
```xml
<dependency>
    <groupId>com.microsoft.azure</groupId>
    <artifactId>azure-servicebus</artifactId>
    <version>3.5.0</version>
</dependency>

<dependency>
    <groupId>com.microsoft.azure</groupId>
    <artifactId>azure-spring-boot-starter</artifactId>
</dependency>
```
 
application.properties
```properties
baldir.craft.azure.servicebus.connection-string=Endpoint=sb://{your service bus namespace name}.servicebus.windows.net/;SharedAccessKeyName={your access key};SharedAccessKey={your_secret_key}
baldir.craft.queue.sample.name=sample-queue
```

Queue configuration 
```java

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
```

Sending message to queue from Rest endpoint
```java

@RestController
public class SendMessageRestController {
    private final IQueueClient sampleQueueClient;


    public SendMessageRestController(IQueueClient sampleQueueClient) {
        this.sampleQueueClient = sampleQueueClient;
    }

    @PostMapping("/send")
    public void sendMessage(@RequestBody String message) throws ServiceBusException, InterruptedException {
        System.out.println("route /send received : "+message);

        System.out.println("Sending message \""+message+"\" to queue");
        sampleQueueClient.send(new Message(message));
    }
}

```

## 02 - add abstractions

First step will be to abstract away The SDK and use interfaces that can hide implementation details (here : azure SDK)
Those interfaces will be provided dependencies from the SDK and then by another provider.




