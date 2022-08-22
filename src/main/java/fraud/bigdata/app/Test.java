package fraud.bigdata.app;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vonage.client.VonageClient;
import com.vonage.client.sms.MessageStatus;
import com.vonage.client.sms.SmsSubmissionResponse;
import com.vonage.client.sms.messages.TextMessage;

import fraud.bigdata.app.entity.Transaction;
import fraud.bigdata.app.entity.TransactionStatus;

public class Test {

	private static String GCP_PROJECT_ID = "ultra-palisade-339421";
	private static String GCP_PROJECT_REGION = "us-central1";
	private static String TRANSACTION_TOPIC = "projects/ultra-palisade-339421/topics/transaction-topic";
	private static String DATASET_ID = "fraud_detection_dataset";
	private static String TABLE_ID = "transaction_fraud_status_data_table";
	private static String FRAUD_STATUS_TOPIC = "projects/ultra-palisade-339421/topics/fraud-status-topic";
	private static String TRANSACTION_STATUS_TOPIC = "projects/ultra-palisade-339421/topics/transaction-status-topic";

	public static void main(String[] args) {

		//convertToJsonTransaction();

		//convertToJsonTransactionStatus();

		runPipleLine();

	}

	public static void convertToJsonTransactionStatus() {
		ObjectMapper objectMapper = new ObjectMapper();
		String jsonString = null;

		TransactionStatus trans = new TransactionStatus();
		trans.setAmount(977.01);
		trans.setCc_number("3524574586339330");
		trans.setLatitude(26.888686);
		trans.setLongitude(-80.834389);
		trans.setRule_fraud_status(1);
		trans.setCategory("shopping_net");
		trans.setTrans_date_and_time("2020-06-21 01:00:08");
		trans.setMl_fraud_status(1);

		try {
			jsonString = objectMapper.writeValueAsString(trans);
			System.out.println("Converted JSON string  : " + jsonString);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}

	public static void runPipleLine() {
		GCPDataflowIntegration dataflow = new GCPDataflowIntegration();
		Pipeline pipeline = dataflow.getPipeline(false, GCP_PROJECT_ID, GCP_PROJECT_REGION);
		PipelineResult result = dataflow.createAndRunPipeline(pipeline, GCP_PROJECT_ID, TRANSACTION_TOPIC, DATASET_ID,
				TABLE_ID, FRAUD_STATUS_TOPIC, TRANSACTION_STATUS_TOPIC);
		System.out.println(result);
	}

	public static void convertToJsonTransaction() {
		ObjectMapper objectMapper = new ObjectMapper();
		String jsonString = null;

		Transaction trans = new Transaction();
		trans.setAmount(9.16);
		trans.setCardNo("123456789");
		trans.setDateTime("2022-01-01 00:00:18");
		trans.setLatitude(82.8);
		trans.setLongitude(82.8);
		trans.setCategory("shopping_net");

		try {
			jsonString = objectMapper.writeValueAsString(trans);
			System.out.println("Converted JSON string  : " + jsonString);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}
	
	private static void connectToVoyageApi() {
		VonageClient client = VonageClient.builder().apiKey("3dd1c735").apiSecret("UWRQTStM4vusGYDQ").build();
		TextMessage message = new TextMessage("Vonage APIs",
		        "4915225884607",
		        "A text message sent using the Vonage SMS API"
		);

		SmsSubmissionResponse response = client.getSmsClient().submitMessage(message);

		if (response.getMessages().get(0).getStatus() == MessageStatus.OK) {
		    System.out.println("Message sent successfully.");
		} else {
		    System.out.println("Message failed with error: " + response.getMessages().get(0).getErrorText());
		}
	}

}
