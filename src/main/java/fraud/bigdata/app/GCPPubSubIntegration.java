package fraud.bigdata.app;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class GCPPubSubIntegration {

	private static String PUBSUB_EMULATOR_HOST = "localhost:8085";

	private Publisher publisher;
	private ManagedChannel channel;
	private Subscriber subscriber;

	/*
	 * Publish a message to PubSub topic.
	 */
	public String publishAMessage(boolean isTest, String projectId, String topicId, String message) throws IOException {
		String messageId = null;
		if (isTest) {
			// Connect to PubSub local emulator
			publisher = getEmulatorPublisher(projectId, topicId);
		} else {
			// Connect to PubSub resources in the Cloud
			publisher = getPublisher(projectId, topicId);
		}
		//send json string https://cloud.google.com/pubsub/docs/publisher#java_1
		ByteString data = ByteString.copyFromUtf8(message);
		PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

		// Once published, returns a server-assigned message id (unique within the
		// topic)
		ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
		try {
			messageId = messageIdFuture.get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		finally {
			if(channel!=null) {
				 channel.shutdown();
			}
			if (publisher != null) {
		        // When finished with the publisher, shutdown to free up resources.
		        publisher.shutdown();
		        try {
					publisher.awaitTermination(1, TimeUnit.MINUTES);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		      }
		}
		System.out.println("Published message ID: " + messageId);
		return messageId;
	}

	/*
	 * Build the publisher to publish message.
	 */
	private Publisher getPublisher(String projectId, String topicId) throws IOException {
		TopicName topicName = TopicName.of(projectId, topicId);
		// Create a publisher instance with default settings bound to the topic
		publisher = Publisher.newBuilder(topicName).build();
		return publisher;
	}

	/*
	 * Build the publisher to publish message to the emulator.
	 */
	private Publisher getEmulatorPublisher(String projectId, String topicId) throws IOException {
		channel = ManagedChannelBuilder.forTarget(PUBSUB_EMULATOR_HOST).usePlaintext().build();
		TransportChannelProvider channelProvider = FixedTransportChannelProvider
				.create(GrpcTransportChannel.create(channel));
		CredentialsProvider credentialsProvider = NoCredentialsProvider.create();

		// Set the channel and credentials provider when creating a `TopicAdminClient`.
		// Similarly for SubscriptionAdminClient
		TopicAdminClient topicClient = TopicAdminClient.create(TopicAdminSettings.newBuilder()
				.setTransportChannelProvider(channelProvider).setCredentialsProvider(credentialsProvider).build());

		TopicName topicName = TopicName.of(projectId, topicId);
		publisher = Publisher.newBuilder(topicName).setChannelProvider(channelProvider)
				.setCredentialsProvider(credentialsProvider).build();
		return publisher;
	}
	
	
	/*
	 * Build the subscriber to subscribe messages from the emulator.
	 */
	private Subscriber getEmulatorSubscriber(ProjectSubscriptionName subscriptionName, MessageReceiver receiver) throws IOException {
		channel = ManagedChannelBuilder.forTarget(PUBSUB_EMULATOR_HOST).usePlaintext().build();
		TransportChannelProvider channelProvider = FixedTransportChannelProvider
				.create(GrpcTransportChannel.create(channel));
		CredentialsProvider credentialsProvider = NoCredentialsProvider.create();

		// Set the channel and credentials provider when creating a `TopicAdminClient`.
		// Similarly for SubscriptionAdminClient
		TopicAdminClient topicClient = TopicAdminClient.create(TopicAdminSettings.newBuilder()
				.setTransportChannelProvider(channelProvider).setCredentialsProvider(credentialsProvider).build());

		subscriber = Subscriber.newBuilder(subscriptionName, receiver).setChannelProvider(channelProvider)
				.setCredentialsProvider(credentialsProvider).build();
		return subscriber;
	}
	
	private Subscriber getSubsscriber(ProjectSubscriptionName subscriptionName, MessageReceiver receiver) {
		subscriber = Subscriber.newBuilder(subscriptionName, receiver).build();
		return subscriber;
	}

	public void subscribeToPubSubSubscription(boolean isTest, String projectId, String subscriptionId) throws IOException {
		ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);
		// Instantiate an asynchronous message receiver.
	    MessageReceiver receiver =
	        (PubsubMessage message, AckReplyConsumer consumer) -> {
	          // Handle incoming message, then ack the received message.
	          System.out.println("Id: " + message.getMessageId());
	          System.out.println("Data: " + message.getData().toStringUtf8());
	          consumer.ack();
	        };

	    Subscriber subscriber = null;
	    
	    if(isTest) {
	    	subscriber = getEmulatorSubscriber(subscriptionName, receiver);
	    }
	    else{
	    	subscriber = getSubsscriber(subscriptionName, receiver);
	    }
	    try {
	      // Start the subscriber.
	      subscriber.startAsync().awaitRunning();
	      System.out.printf("Listening for messages on %s:\n", subscriptionName.toString());
	      // Allow the subscriber to run for 30s unless an unrecoverable error occurs.
	      subscriber.awaitTerminated(30, TimeUnit.SECONDS);
	    } catch (TimeoutException timeoutException) {
	      // Shut down the subscriber after 30s. Stop receiving messages.
	      subscriber.stopAsync();
	    }
	}

}
