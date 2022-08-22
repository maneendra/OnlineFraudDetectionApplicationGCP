package fraud.bigdata.app;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class GCPPubSubIntegrationTest {

	GCPPubSubIntegration pubsub;

	private static String GCP_PROJECT_ID = "ultra-palisade-339421";
	private static String PUBUB_TOPIC_ID = "transaction-topic";
	private static String PUBUB_RESULT_TOPIC_ID = "fraud-status-topic";
	private static String PUBSUB_SUBSCRIPTION_ID = "transaction-pull-subscription";

	private String message;

	private final PrintStream standardOut = System.out;
	private final ByteArrayOutputStream outputStreamCaptor = new ByteArrayOutputStream();

	@Before
	public void setUp() {
		pubsub = new GCPPubSubIntegration();
		message = "{\r\n"
				+ "    \"distance\": 82.8,\r\n"
				+ "    \"amt\": 9.16,\r\n"
				+ "    \"unix_time\": 1608428547,\r\n"
				+ "}";

		
	}

	@Test
	public void testPublishingMessagesToPubSubTopic() throws IOException {
		String messageId = pubsub.publishAMessage(false, GCP_PROJECT_ID, PUBUB_TOPIC_ID, message);
		assertFalse(messageId.isEmpty());
	}

	@Test
	public void testSubscribingMessagesFromPubSubPushSubscription() throws IOException {
		System.setOut(new PrintStream(outputStreamCaptor));
		
		pubsub.publishAMessage(false, GCP_PROJECT_ID, PUBUB_TOPIC_ID, message);
		pubsub.subscribeToPubSubSubscription(false, GCP_PROJECT_ID, PUBSUB_SUBSCRIPTION_ID);
		assertTrue(outputStreamCaptor.toString().contains("{\r\n"
				+ "    \"distance\": 82.8,\r\n"
				+ "    \"amt\": 9.16,\r\n"
				+ "    \"unix_time\": 1608428547,\r\n"
				+ "}"));
		
		System.setOut(standardOut);

	}
	
	//@Test
	//https://cloud.google.com/pubsub/docs/emulator#windows
	public void testPublishingMessagesToPubSubTopicUsingEmulator() throws IOException {
		String messageId = pubsub.publishAMessage(true, GCP_PROJECT_ID, PUBUB_TOPIC_ID, message);
		assertFalse(messageId.isEmpty());
	}

	//@Test
	public void testSubscribingMessagesFromPubSubPushSubscriptionUsingEmulator() throws IOException {
		System.setOut(new PrintStream(outputStreamCaptor));
		
		pubsub.publishAMessage(true, GCP_PROJECT_ID, PUBUB_TOPIC_ID, message);
		pubsub.subscribeToPubSubSubscription(true, GCP_PROJECT_ID, PUBSUB_SUBSCRIPTION_ID);
		assertTrue(outputStreamCaptor.toString().contains("{\r\n"
				+ "    \"distance\": 82.8,\r\n"
				+ "    \"amt\": 9.16,\r\n"
				+ "    \"unix_time\": 1608428547,\r\n"
				+ "}"));
		
		System.setOut(standardOut);

	}
	
	
	@Test
	public void testWriteToPubSub() throws IOException {
		message = "{\"fraud_status\": \"1\", \"amt\": 15.56, \"category\": \"entertainment\", \"cc_num\": \"30263540414123\", \"lat\": 36.841266, \"long\":-111.69076499999998, \"trans_date_trans_time\":\"2020-06-21 12:12:08\"}";
		String messageId = pubsub.publishAMessage(false, GCP_PROJECT_ID, PUBUB_RESULT_TOPIC_ID, message);
		assertNotNull(messageId);
	}

	@After
	public void tearDown() {
		
	}

}
