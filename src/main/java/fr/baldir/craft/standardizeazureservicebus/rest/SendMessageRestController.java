package fr.baldir.craft.standardizeazureservicebus.rest;

import com.microsoft.azure.servicebus.IQueueClient;
import com.microsoft.azure.servicebus.Message;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

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
