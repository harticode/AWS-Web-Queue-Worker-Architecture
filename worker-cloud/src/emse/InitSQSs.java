package emse;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

public class InitSQSs {
	
	public static void InitSQS() {
		//The default Region in my case, feel free to change it 
	    Region region = Region.EU_CENTRAL_1;
	    
	    final String HOWTOUSE = "\n" +
                "Usage:\n" +
                "   Set in the Code  <queueInbox>\n" +
                "   Set in the Code  <queueOutbox>\n" +
                "Where:\n" +
                "   queueInbox - AWS SQS queue named Inbox to receive messages from clients\n" +
                "   queueOutbox - AWS SQS queue named Outbox to send messages to clients\n";

	    String queueInbox = "Inbox";
	    String queueOutbox = "Outbox";


	    
	    if (queueInbox.length() == 0 || queueOutbox.length() == 0) {
	        System.out.println(HOWTOUSE);
	        System.exit(1);
   		}
	    
	    //init SQS
	    final SqsClient sqs = SqsClient.builder().region(region).build();
	    
	    //Create the Queue in the Amazon SQS
		createQueue(sqs, queueInbox);
		createQueue(sqs, queueOutbox);


	}
	
	public static void createQueue(SqsClient sqsClient,String queueName ) {

        try {
            System.out.println("\nCreate Queue");
            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();

            sqsClient.createQueue(createQueueRequest);

            System.out.println("\n"+queueName+" queue created");
        } catch (SqsException e){
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }

    }
	

}
