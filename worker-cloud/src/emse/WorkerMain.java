package emse;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

//Structure for Country and Product Combination
class CcountryAndproduct {
    public String country;
    public String product;
    public int count;
    
 // constructor
    public CcountryAndproduct(String country, String product) {
       this.country = country;
       this.product = product;
       this.count = 0;
    }

}


public class WorkerMain {

	public static void main(String[] args) {
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
	    
	    String bucket = "";
	    String incomeFile = "";
	    
	    if (queueInbox.length() == 0 || queueOutbox.length() == 0) {
	        System.out.println(HOWTOUSE);
	        System.exit(1);
   		}
	    
	    //init S3 and SQS
	    final S3Client s3 = S3Client.builder().region(region).build();
	    final SqsClient sqs = SqsClient.builder().region(region).build();
	    
	    //Check if the SQS and S3 exist or not
	    if (!QueueExist(sqs, queueInbox) || !QueueExist(sqs, queueOutbox)) {
	        System.out.println("Init Queues");
	        InitSQSs.InitSQS();
   		}

    	//Get Queues URLs
    	String m_InboxUrl = GetQueueUrl(sqs, queueInbox);
    	String m_OutboxUrl = GetQueueUrl(sqs, queueOutbox);
		
		
		while(true) {
			//init Result lines
			List<String> lines = new ArrayList<>();
			//Get Clients Request from SQS
			String[] clientRequest = WaitResponseFromClients(sqs, m_InboxUrl);
			bucket = clientRequest[0];
			incomeFile = clientRequest[1];
			System.out.println("\nbucket is " + bucket + " and FileName is " + incomeFile);
			
			//Get S3 Object
		    byte[] data = GetS3Object(s3, bucket, incomeFile, sqs, m_OutboxUrl);
		    
		    String[] listcsvString = new String(data, StandardCharsets.UTF_8).split("\n"); 
		  //Total Number of Sales
			lines.add("\nTotal Number of Sales is = "+ (listcsvString.length-1));
			
			//Total amount Sold by Country and Product combination
			lines.add("\nTotal Amount Sold by Country and Product = "+ (listcsvString.length-1));
			List<CcountryAndproduct> combinations = new ArrayList<CcountryAndproduct>();
			for (String country : GetUniqueCountryList(listcsvString)) {
				for (String product : GetUniqueProductList(listcsvString)) {
					CcountryAndproduct element = new CcountryAndproduct(country, product);
					combinations.add(element);
				}
			}
			for(int i=1 ; i<listcsvString.length;i++) {
				for(CcountryAndproduct ele: combinations) {
					if(listcsvString[i].contains(ele.country) & listcsvString[i].contains(ele.product)) {
						ele.count = ele.count + 1;
						break;
					}
				}
			}
			
			for(CcountryAndproduct ele: combinations) {
				lines.add("\n>>>"+ele.country+" - "+ele.product+" : "+ele.count);
			}
			
			//Upload The FILE to S3 Bucket
	    	String outputfile = putS3String(s3, bucket, incomeFile, lines);
	    	
			//Sending a message containing the name of the bucket and file to the queue to Worker
	    	String m_Message = incomeFile+";"+outputfile;
		    sendMessage(sqs,m_OutboxUrl, m_Message);

		}
	}
	
	public static List<Message> receiveMessages(SqsClient sqsClient, String queueUrl) {
        try {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(5)
                .build();
            List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
            return messages;
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return null;
    }
	
	public static void deleteMessage(SqsClient sqsClient, String queueUrl,  Message message) {
        System.out.println("\nDelete Message "+ message.body());

        try {
        	DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build();
                sqsClient.deleteMessage(deleteMessageRequest);
            System.out.println("\nMessage has been deteled");

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
   
	}
	
	public static String[] WaitResponseFromClients(SqsClient sqsClient, String queueUrl) {
		Boolean m_recieved = false;
		String m_bucket = "";
		String m_outputFile = "";
		System.out.println("\nTrying to Receive messages From :" + queueUrl);
		
		while(!m_recieved) {
			try {
			    TimeUnit.SECONDS.sleep(60);  // Wait 60 seconds
			} catch (InterruptedException e) {
			    e.printStackTrace();
			}
	    	List<Message> messages = receiveMessages(sqsClient, queueUrl);
	    	if(!messages.isEmpty()) {
			    for (Message msg : messages) {
			    	String[] parts = msg.body().split(";");
			    	m_bucket = parts[0];
			    	m_outputFile = parts[1];
		        	
		        	//Delete Message of the Queue
		        	deleteMessage(sqsClient, queueUrl, msg);
		        }
			    m_recieved = true;
		    }
		}
		return new String[] {m_bucket, m_outputFile};
    }
	
	public static Boolean QueueExist(SqsClient sqsClient, String queueName) {
        try {
            sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            return true;
        } catch (QueueDoesNotExistException e){
            return false;
        }
    }
	
	public static Boolean S3bucketExist(S3Client s3, String bucketName) {
		HeadBucketRequest headBucketRequest = HeadBucketRequest.builder()
	            .bucket(bucketName)
	            .build();

	    try {
	        s3.headBucket(headBucketRequest);
	        return true;
	    } catch (NoSuchBucketException e) {
	        return false;
	    }
	}
	
	public static String GetQueueUrl(SqsClient sqsClient,String queueName ) {
        try {
            System.out.println("\nGet "+queueName+" url");
            GetQueueUrlResponse getQueueUrlResponse =
                sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            String queueUrl = getQueueUrlResponse.queueUrl();
            System.out.println("queueUrl is = " + queueUrl);
            return queueUrl;
        } catch (SqsException e){
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
    }
	
	public static byte[] GetS3Object(S3Client s3, String bucketName, String filename, SqsClient sqsClient,String queueUrl) {
		try {
			GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(filename)
                    .bucket(bucketName)
                    .build();
			
			ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(objectRequest);
            byte[] data = objectBytes.asByteArray();
			return data;	    
		
		} catch (S3Exception e) {
			System.err.println(e.getMessage());
			sendMessage(sqsClient, queueUrl, "notFound");
		}
		return null;
	}
	
	public static void sendMessage(SqsClient sqsClient,String queueUrl, String msg) {
		System.out.println("\nSend message");

        try {
            sqsClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(msg)
                .delaySeconds(2)
                .build());
            System.out.println("\nmessage has been sent");
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
	}
	
	public static String putS3String(S3Client s3,
		     String bucketName,
		     String filename,
		     List<String> lines) {
		
		try {
			filename = filename + "-worker-result.txt";
			Map<String, String> metadata = new HashMap<>();
			metadata.put("cloud_computing", "lab3");
			
			PutObjectRequest putOb = PutObjectRequest.builder()
										.bucket(bucketName)
										.key(filename)
										.metadata(metadata)
										.build();
			
			PutObjectResponse response = s3.putObject(putOb,
			RequestBody.fromBytes(StringtoBytes(lines)));

			System.out.println("\n"+filename+" has been Uploaded to "+ bucketName);
			return filename;
		
		} catch (S3Exception e) {
			System.err.println(e.getMessage());
		}
	return "";
	}
	
	public static byte[] StringtoBytes(List<String> lines) {
		StringBuffer buffer = new StringBuffer();
		for(String line: lines){
		   buffer.append(line); //error here
		}
		byte[] bytes = buffer.toString().getBytes();
		
		return bytes;
	}
	
	public static Set<String> GetUniqueProductList(String[] data) {
		Set<String> m_Products = new HashSet<String>();
		for(int i=1 ; i<data.length;i++) {
			m_Products.add(data[i].split(",")[2]);
		}
		return m_Products;
	}
	
	public static Set<String> GetUniqueCountryList(String[] data) {
		Set<String> m_Countries = new HashSet<String>();
		for(int i=1 ; i<data.length;i++) {
			m_Countries.add(data[i].split(",")[8]);
		}
		return m_Countries;
	}
}
