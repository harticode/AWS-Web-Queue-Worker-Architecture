package emse;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

public class ClientMain {

	public static void main(String[] args) {
		Region region = Region.EU_CENTRAL_1;
		
		final String INITHOWTOUSE = "\n Do Not Forget to run initSQSs file before to create"+
				"   queueInbox - AWS SQS queue named Inbox to receive messages from clients\n" +
                "   queueOutbox - AWS SQS queue named Outbox to send messages to clients\n";
	    
	    final String HOWTOUSE = "\n" +
                "Usage:\n" +
                "   Set in the Code  <queueInbox>\n" +
                "   Set in the Code  <queueOutbox>\n" +
                "   Set in the Code  <bucket>\n" +
                "   Set in the Code  <filename>\n" +
                "   Set in the Code  <filePath>\n" +
                "Where:\n" +
                "   queueInbox - AWS SQS queue named Inbox to receive messages from clients\n" +
                "   queueOutbox - AWS SQS queue named Outbox to send messages to clients\n" +
                "   bucket - Set a name for your bucket in init file\n" +
                "   filename - Find the name of the file in the system\n" +
                "   filePath - Get the path of the file you want to send in the system\n";

	    String queueInbox = "Inbox";
	    String queueOutbox = "Outbox";
	    String bucket = "hamzaaitbaalilab3";
	    String filename = "sales-2021-01-02.csv";
	    String filePath = "D:\\Users\\harti\\Downloads\\"+filename;
	    
	    String incomeFile = "";
	    String outputFile = "";
	    System.out.println("started");
	    if (queueInbox.length() == 0 || queueOutbox.length() == 0 || bucket.length() == 0 || filename.length() == 0 || filePath.length() == 0) {
	        System.out.println(HOWTOUSE);
	        System.exit(1);
   		}
	    
	    //init S3 and SQS
	    final S3Client s3 = S3Client.builder().region(region).build();
	    final SqsClient sqs = SqsClient.builder().region(region).build();
	    
	    //Check if the SQS and S3 exist or not
	    if (!QueueExist(sqs, queueInbox) || !QueueExist(sqs, queueOutbox)) {
	        System.out.println(INITHOWTOUSE);
	        System.exit(1);
   		}
	    
	    //Create Bucket S3
	    //Check if bucket exist already or not
	    if(!S3bucketExist(s3, bucket)) {
	    	createBucket(s3, bucket);
	    }
	    
	  
	    //Upload The FILE to S3 Bucket
    	putS3Object(s3, bucket, filename, filePath);
    	
    	//Get Queues URLs
    	String m_InboxUrl = GetQueueUrl(sqs, queueInbox);
    	String m_OutboxUrl = GetQueueUrl(sqs, queueOutbox);
    	
    	//The Message( the delimiter in a semicolon )
	    String m_Message = bucket + ";" + filename;
    	
    	//Sending a message containing the name of the bucket and file to the queue to Worker
	    sendMessage(sqs,m_InboxUrl, m_Message);
	    
	    //Wait Response from Worker
	    String[] response = WaitResponseFromWorker(sqs, m_OutboxUrl);
	    incomeFile = response[0];
	    outputFile = response[1];
	    
	    //Check if the Worker got the Right Filename
	    if(!incomeFile.equals(filename)){
    		System.out.println("Looks like the worker got the wrong filename");
    		System.exit(1);
    	}
	    //Download the output File from S3
	    System.out.println("\nFile we sent is " + incomeFile + " and output file is " + outputFile);
	    String outputPath = "D:\\Users\\harti\\Downloads\\"+outputFile;
	    saveS3ObjecttoFile(s3, bucket, outputFile, outputPath);

	}
	
	
	public static void createBucket( S3Client s3Client, String bucketName) {

        try {
            S3Waiter s3Waiter = s3Client.waiter();
            CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            s3Client.createBucket(bucketRequest);
            HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            // Wait until the bucket is created and print out the response.
            WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
            waiterResponse.matched().response().ifPresent(System.out::println);
            System.out.println("\n\nThe bucket " + bucketName + " is ready");

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
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
	
	public static String putS3Object(S3Client s3,
		     String bucketName,
		     String filename,
		     String objectPath) {

		try {
			Map<String, String> metadata = new HashMap<>();
			metadata.put("cloud_computing", "lab3");
			
			PutObjectRequest putOb = PutObjectRequest.builder()
										.bucket(bucketName)
										.key(filename)
										.metadata(metadata)
										.build();
			
			PutObjectResponse response = s3.putObject(putOb,
			RequestBody.fromBytes(getObjectFile(objectPath)));

			System.out.println("\n"+filename+" has been Uploaded to "+ bucketName);
			return response.eTag();
		
		} catch (S3Exception e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
	return "";
	}
	
	// Return a byte array
	private static byte[] getObjectFile(String filePath) {
	
		FileInputStream fileInputStream = null;
		byte[] bytesArray = null;
		
		try {
			File file = new File(filePath);
			bytesArray = new byte[(int) file.length()];
			fileInputStream = new FileInputStream(file);
			fileInputStream.read(bytesArray);
			
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
			if (fileInputStream != null) {
				try {
					fileInputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return bytesArray;
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
            System.exit(1);
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
            System.exit(1);
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
            System.exit(1);
        }
   
	}
	
	public static String[] WaitResponseFromWorker(SqsClient sqsClient, String queueUrl) {
		Boolean m_recieved = false;
		String m_incomeFile = "";
		String m_outputFile = "";
		System.out.println("\nTrying to Receive messages From :" + queueUrl);
		while(!m_recieved) {
			try {
			    TimeUnit.SECONDS.sleep(10);  // Wait 10 seconds
			} catch (InterruptedException e) {
			    e.printStackTrace();
			}
	    	List<Message> messages = receiveMessages(sqsClient, queueUrl);
	    	if(!messages.isEmpty()) {
			    for (Message msg : messages) {
			    	if(msg.body().equals("notFound")) {
			    		System.out.println("the File sent is not found by the worker");
			    		//Delete Message of the Queue
			        	deleteMessage(sqsClient, queueUrl, msg);
			    		System.exit(1);
			    	}
			    	String[] parts = msg.body().split(";");
			    	m_incomeFile = parts[0];
			    	m_outputFile = parts[1];
		        	
		        	//Delete Message of the Queue
		        	deleteMessage(sqsClient, queueUrl, msg);
		        }
			    m_recieved = true;
		    }
		}
		return new String[] {m_incomeFile, m_outputFile};
    }
	
	public static void saveS3ObjecttoFile (S3Client s3, String bucketName, String keyName, String path ) {

        try {
            GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(keyName)
                    .bucket(bucketName)
                    .build();

            ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(objectRequest);
            byte[] data = objectBytes.asByteArray();
            System.out.println("Successfully obtained bytes from an S3 object");

            // Write the data to a local file
            File myFile = new File(path);
            OutputStream os = new FileOutputStream(myFile);
            os.write(data);
            System.out.println("Successfully saved to this computer");
            os.close();

        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (S3Exception e) {
          System.err.println(e.awsErrorDetails().errorMessage());
           System.exit(1);
        }
    }


}
