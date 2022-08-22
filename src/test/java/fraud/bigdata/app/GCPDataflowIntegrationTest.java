package fraud.bigdata.app;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.api.services.bigquery.model.TableRow;

import fraud.bigdata.app.entity.Transaction;
import fraud.bigdata.app.entity.TransactionStatus;

@ExtendWith(MockitoExtension.class)
public class GCPDataflowIntegrationTest {
	
	private GCPDataflowIntegration dataflow;
	
	private static String GCP_PROJECT_ID = "ultra-palisade-339421";
	private static String GCP_PROJECT_REGION = "us-central1";
	private static String TRANSACTION_TOPIC = "projects/ultra-palisade-339421/topics/transaction-topic";
	private static String DATASET_ID = "fraud_detection_dataset";
	private static String TABLE_ID = "transaction_fraud_status_data_table";
	private static String FRAUD_STATUS_TOPIC = "projects/ultra-palisade-339421/topics/fraud-status-topic";
	private static String TRANSACTION_STATUS_TOPIC = "projects/ultra-palisade-339421/topics/transaction-status-topic";
	
	//Test pipline to test transformers
	//https://beam.apache.org/documentation/pipelines/test-your-pipeline/
	private String message;
	
	private String messageWithCategory;
	
	@Mock
	HttpClient mockHttpClient;
	@Mock
	HttpResponse mockHttpResponse;
	@Mock
	HttpRequest httpPost;
	
	@Before
	public void setUp() {
		dataflow = new GCPDataflowIntegration();
		//pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
		
		message = "{\r\n"
				+ "    \"latitude\": 82.8,\r\n"
				+ "	\"longitude\": 82.8,\r\n"
				+ "    \"amount\": 9.16,\r\n"
				+ "    \"dateTime\": \"1608428547\",\r\n"
				+ "	\"cardNo\" : \"123456789\",\r\n"
				+ "    \"category\": \"gas_transport\",\r\n"
				+ "	\r\n"
				+ "}";
		
		mockHttpClient = HttpClient.newHttpClient();
		mockHttpResponse = Mockito.mock(HttpResponse.class);
		httpPost = Mockito.mock(HttpRequest.class);
	}
	
	@Test
	//Running the pipeline locally for testing
	//https://cloud.google.com/dataflow/docs/guides/setting-pipeline-options#setting-other-cloud-pipeline-options- Launching locally
	public void testCreatingPipeline() {
		assertNotNull(dataflow.getPipeline(true, GCP_PROJECT_ID, GCP_PROJECT_REGION));
	}
	
	
	/*
	 * Test transforming PubSub JSON message to Transaction object
	 */
	@Test
	public void testTransformPubSubMessageToTransactionObject() {
		Pipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
		byte[] data = message.getBytes();
		PubsubMessage pubsubMessage = new PubsubMessage(data, null);
		List<PubsubMessage> msgList = new ArrayList<>();
		msgList.add(pubsubMessage);
		
		PCollection<PubsubMessage> pubSubInput = pipeline.apply(Create.of(msgList));
		PCollection<Transaction> pubSubOutput = pubSubInput.apply(new ReadPubSubMessages());
		
		Transaction trans = new Transaction();
		trans.setAmount(9.16);
		trans.setCardNo("123456789");
		trans.setDateTime("1608428547");
		trans.setLatitude(82.8);
		trans.setLongitude(82.8);
		
		
		PAssert.that(pubSubOutput).containsInAnyOrder(trans);
		
		PipelineResult result = pipeline.run();
		System.out.println(result.getState());
		System.err.println(result.metrics());
		}
	
	
	/*
	 * Test the transaction validity via InvokeRuleEngine PTransform.
	 * Scenario 1 - transaction is valid
	 */
	@Test
	public void testInvokingTheRuleEngineWhenTransactionIsValid() {
		Pipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
		
		Transaction trans = new Transaction();
		trans.setAmount(9.16);
		trans.setCardNo("123456789");
		trans.setDateTime("2022-01-01 00:00:18");
		trans.setLatitude(82.8);
		trans.setLongitude(82.8);
		List<Transaction> transList = new ArrayList<>();
		transList.add(trans);
		
		PCollection<Transaction> transInput = pipeline.apply(Create.of(transList));
		PCollection<TransactionStatus> ruleEngineOutput = transInput.apply(new InvokeRuleEngine());
		
		TransactionStatus ruleObj = new TransactionStatus();
		ruleObj.setAmount(9.16);
		ruleObj.setCc_number("123456789");
		ruleObj.setLatitude(82.8);
		ruleObj.setLongitude(82.8);
		ruleObj.setMl_fraud_status(0);
		ruleObj.setRule_fraud_status(1);
		ruleObj.setTrans_date_and_time("2022-01-01 00:00:18");
		
		PAssert.that(ruleEngineOutput).containsInAnyOrder(ruleObj);
		
		PipelineResult result = pipeline.run();
		System.out.println(result.getState());
		System.err.println(result.metrics());
		
	}
	
	/*
	 * Test the transaction validity via InvokeRuleEngine PTransform. 
	 * Scenario 2 - transaction is invalid
	 */
	@Test
	public void testInvokingTheRuleEngineWhenWhenTransactionIsInValid() {
		Pipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
		
		Transaction trans = new Transaction();
		trans.setAmount(1500);
		trans.setCardNo("123456789");
		trans.setDateTime("1608428547");
		trans.setLatitude(82.8);
		trans.setLongitude(82.8);
		List<Transaction> transList = new ArrayList<>();
		transList.add(trans);
		
		PCollection<Transaction> transInput = pipeline.apply(Create.of(transList));
		PCollection<TransactionStatus> ruleEngineOutput = transInput.apply(new InvokeRuleEngine());
		
		TransactionStatus ruleObj = new TransactionStatus();
		ruleObj.setAmount(1500);
		ruleObj.setCc_number("123456789");
		ruleObj.setLatitude(82.8);
		ruleObj.setLongitude(82.8);
		ruleObj.setMl_fraud_status(0);
		ruleObj.setRule_fraud_status(0);
		ruleObj.setTrans_date_and_time("1608428547");
		
		PAssert.that(ruleEngineOutput).containsInAnyOrder(ruleObj);
		
		PipelineResult result = pipeline.run();
		System.out.println(result.getState());
		System.err.println(result.metrics());
		
	}
	
	
	/*
	 * Test invoking ML model and transforming result when the prediction is not fraud.
	 */
	@Test
	public void testInvokingMLModelWhenPredictionIsGenuine() {
		Pipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
		
		/*Transaction trans = new Transaction();
		trans.setCardNo("30263540414123");
		trans.setAmount(15.56);
		trans.setLongitude(-111.69076499999998);
		trans.setLatitude(36.841266);
		trans.setDateTime("2020-06-21 12:12:08");
		trans.setCategory("entertainment");*/
		
		TransactionStatus trans = new TransactionStatus();
		trans.setAmount(15.56);
		trans.setCc_number("30263540414123");
		trans.setLatitude(36.841266);
		trans.setLongitude(-111.69076499999998);
		trans.setRule_fraud_status(1);
		trans.setCategory("entertainment");
		trans.setTrans_date_and_time("2020-06-21 12:12:08");
		
		List<TransactionStatus> transList = new ArrayList<>();
		transList.add(trans);
		
		PCollection<TransactionStatus> transInput = pipeline.apply(Create.of(transList));
		PCollection<TransactionStatus> mlOutput = transInput.apply(new InvokeMLModel());
		
		TransactionStatus mlObj = new TransactionStatus();
		mlObj.setAmount(15.56);
		mlObj.setCc_number("30263540414123");
		mlObj.setLatitude(36.841266);
		mlObj.setLongitude(-111.69076499999998);
		mlObj.setMl_fraud_status(0);
		mlObj.setRule_fraud_status(1);
		mlObj.setCategory("entertainment");
		mlObj.setTrans_date_and_time("2020-06-21 12:12:08");
		
		PAssert.that(mlOutput).containsInAnyOrder(mlObj);
		
		PipelineResult result = pipeline.run();
		System.out.println(result.getState());
		System.err.println(result.metrics());
		
	}
	
	
	/*
	 * Test invoking ML model and transforming result when the prediction is fraud.
	 */
	@Test
	public void testInvokingMLModelWhenPredictionIsFraud() {
		Pipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
		
		/*Transaction trans = new Transaction();
		trans.setAmount(977.01);
		trans.setCardNo("3524574586339330");
		trans.setDateTime("2020-06-21 01:00:08");
		trans.setLatitude(26.888686);
		trans.setLongitude(-80.834389);
		trans.setCategory("shopping_net");*/
		
		TransactionStatus trans = new TransactionStatus();
		trans.setAmount(977.01);
		trans.setCc_number("3524574586339330");
		trans.setLatitude(26.888686);
		trans.setLongitude(-80.834389);
		trans.setRule_fraud_status(1);
		trans.setCategory("shopping_net");
		trans.setTrans_date_and_time("2020-06-21 01:00:08");
		
		List<TransactionStatus> transList = new ArrayList<>();
		transList.add(trans);
		
		PCollection<TransactionStatus> transInput = pipeline.apply(Create.of(transList));
		PCollection<TransactionStatus> mlOutput = transInput.apply(new InvokeMLModel());
		
		TransactionStatus mlObj = new TransactionStatus();
		mlObj.setAmount(977.01);
		mlObj.setCc_number("3524574586339330");
		mlObj.setLatitude(26.888686);
		mlObj.setLongitude(-80.834389);
		mlObj.setMl_fraud_status(1);
		mlObj.setRule_fraud_status(1);
		mlObj.setCategory("shopping_net");
		mlObj.setTrans_date_and_time("2020-06-21 01:00:08");
		
		PAssert.that(mlOutput).containsInAnyOrder(mlObj);
		
		PipelineResult result = pipeline.run();
		System.out.println(result.getState());
		System.err.println(result.metrics());
		
	}
	
	
	/*
	 * Test transforming TransactionStatus object to Bigquery Table row
	 */
	@Test
	public void testTransformingToTableRow() {
		
		Pipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
	
		TransactionStatus trans = new TransactionStatus();
		trans.setAmount(977.01);
		trans.setCc_number("3524574586339330");
		trans.setLatitude(26.888686);
		trans.setLongitude(-80.834389);
		trans.setRule_fraud_status(1);
		trans.setCategory("shopping_net");
		trans.setTrans_date_and_time("2020-06-21 01:00:08");
		trans.setMl_fraud_status(1);
		
		List<TransactionStatus> transList = new ArrayList<>();
		transList.add(trans);
		
		PCollection<TransactionStatus> transInput = pipeline.apply(Create.of(transList));
		PCollection<TableRow> bqOutput = transInput.apply(new WriteToBigQuery());
		
		TableRow row1 = new TableRow();
		row1.set("amount", 977.01);
		row1.set("category", "shopping_net");
		row1.set("longitude", -80.834389);
		row1.set("latitude", 26.888686);
		row1.set("cc_number", "3524574586339330");
		row1.set("trans_date_and_time", "2020-06-21 01:00:08");
		row1.set("ml_fraud_status", 1);
		row1.set("rule_fraud_status", 1);
		
		PAssert.that(bqOutput).containsInAnyOrder(row1);
		
		PipelineResult result = pipeline.run();
		System.out.println(result.getState());
		System.err.println(result.metrics());
		
	}
	
	
	/*
	 * Tests transforming TransactionStatus object to JSON
	 */
	@Test
	public void testTransformingToPubSubJsonMessage() {
		
		Pipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
	
		TransactionStatus trans = new TransactionStatus();
		trans.setAmount(977.01);
		trans.setCc_number("3524574586339330");
		trans.setLatitude(26.888686);
		trans.setLongitude(-80.834389);
		trans.setRule_fraud_status(1);
		trans.setCategory("shopping_net");
		trans.setTrans_date_and_time("2020-06-21 01:00:08");
		trans.setMl_fraud_status(1);
		
		List<TransactionStatus> transList = new ArrayList<>();
		transList.add(trans);
		
		PCollection<TransactionStatus> transInput = pipeline.apply(Create.of(transList));
		PCollection<String> pubSubOutput = transInput.apply(new WriteToPubSub());
	
		String outString = "{\"amount\":977.01,\"category\":\"shopping_net\",\"longitude\":-80.834389,\"latitude\":26.888686,\"cc_number\":\"3524574586339330\",\"trans_date_and_time\":\"2020-06-21 01:00:08\",\"ml_fraud_status\":1,\"rule_fraud_status\":1}";
		PAssert.that(pubSubOutput).containsInAnyOrder(outString);
		
		PipelineResult result = pipeline.run();
		System.out.println(result.getState());
		System.err.println(result.metrics());
		
	}
	
	
	/*
	 * Test filtering fraud transactions.
	 * Scenario 1 - 
	 * rule engine status = 1(genuine)
	 * ml model status = 1(fraud)
	 */
	@Test
	public void testFilteringTransactionsForFraudScenario1() {
		
		Pipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
	
		TransactionStatus trans = new TransactionStatus();
		trans.setAmount(977.01);
		trans.setCc_number("3524574586339330");
		trans.setLatitude(26.888686);
		trans.setLongitude(-80.834389);
		trans.setRule_fraud_status(1);
		trans.setCategory("shopping_net");
		trans.setTrans_date_and_time("2020-06-21 01:00:08");
		trans.setMl_fraud_status(1);
		
		List<TransactionStatus> transList = new ArrayList<>();
		transList.add(trans);
		
		PCollection<TransactionStatus> transInput = pipeline.apply(Create.of(transList));
		PCollection<TransactionStatus> pubSubOutput = transInput.apply(new FilterFraudTransactions());
	
		PAssert.that(pubSubOutput).containsInAnyOrder(trans);
		
		PipelineResult result = pipeline.run();
		System.out.println(result.getState());
		System.err.println(result.metrics());
		
	}
	
	
	/*
	 * Test filtering fraud transactions.
	 * Scenario 2 - 
	 * rule engine status = 1(genuine)
	 * ml model status = 0(genuine)
	 */
	@Test
	public void testFilteringTransactionsForFraudScenario2() {
		
		Pipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
	
		TransactionStatus trans = new TransactionStatus();
		trans.setAmount(977.01);
		trans.setCc_number("3524574586339330");
		trans.setLatitude(26.888686);
		trans.setLongitude(-80.834389);
		trans.setRule_fraud_status(1);
		trans.setCategory("shopping_net");
		trans.setTrans_date_and_time("2020-06-21 01:00:08");
		trans.setMl_fraud_status(0);
		
		List<TransactionStatus> transList = new ArrayList<>();
		transList.add(trans);
		
		PCollection<TransactionStatus> transInput = pipeline.apply(Create.of(transList));
		PCollection<TransactionStatus> pubSubOutput = transInput.apply(new FilterFraudTransactions());
	
		PAssert.that(pubSubOutput).empty();
		
		PipelineResult result = pipeline.run();
		System.out.println(result.getState());
		System.err.println(result.metrics());
		
	}
	
	
	/*
	 * Test filtering fraud transactions.
	 * Scenario 3 - 
	 * rule engine status = 0(fraud)
	 * ml model status = 1(fraud)
	 */
	@Test
	public void testFilteringTransactionsForFraudScenario3() {
		
		Pipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
	
		TransactionStatus trans = new TransactionStatus();
		trans.setAmount(977.01);
		trans.setCc_number("3524574586339330");
		trans.setLatitude(26.888686);
		trans.setLongitude(-80.834389);
		trans.setRule_fraud_status(0);
		trans.setCategory("shopping_net");
		trans.setTrans_date_and_time("2020-06-21 01:00:08");
		trans.setMl_fraud_status(1);
		
		List<TransactionStatus> transList = new ArrayList<>();
		transList.add(trans);
		
		PCollection<TransactionStatus> transInput = pipeline.apply(Create.of(transList));
		PCollection<TransactionStatus> pubSubOutput = transInput.apply(new FilterFraudTransactions());
	
		PAssert.that(pubSubOutput).containsInAnyOrder(trans);
		
		PipelineResult result = pipeline.run();
		System.out.println(result.getState());
		System.err.println(result.metrics());
		
	}
	
	
	/*
	 * Test filtering fraud transactions.
	 * Scenario 4 - 
	 * rule engine status = 0(fraud)
	 * ml model status = 0(genuine)
	 */
	@Test
	public void testFilteringTransactionsForFraudScenario4() {
		
		Pipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
	
		TransactionStatus trans = new TransactionStatus();
		trans.setAmount(977.01);
		trans.setCc_number("3524574586339330");
		trans.setLatitude(26.888686);
		trans.setLongitude(-80.834389);
		trans.setRule_fraud_status(0);
		trans.setCategory("shopping_net");
		trans.setTrans_date_and_time("2020-06-21 01:00:08");
		trans.setMl_fraud_status(0);
		
		List<TransactionStatus> transList = new ArrayList<>();
		transList.add(trans);
		
		PCollection<TransactionStatus> transInput = pipeline.apply(Create.of(transList));
		PCollection<TransactionStatus> pubSubOutput = transInput.apply(new FilterFraudTransactions());
	
		PAssert.that(pubSubOutput).containsInAnyOrder(trans);
		
		PipelineResult result = pipeline.run();
		System.out.println(result.getState());
		System.err.println(result.metrics());
		
	}
	
	
	/*
	 * Test filtering genuine transactions.
	 * Scenario 1 - 
	 * rule engine status = 1(genuine)
	 * ml model status = 1(fraud)
	 */
	@Test
	public void testFilteringTransactionsForGenuineScenario1() {
		
		Pipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
	
		TransactionStatus trans = new TransactionStatus();
		trans.setAmount(97.01);
		trans.setCc_number("3524574586339330");
		trans.setLatitude(26.888686);
		trans.setLongitude(-80.834389);
		trans.setRule_fraud_status(1);
		trans.setCategory("shopping_net");
		trans.setTrans_date_and_time("2020-06-21 01:00:08");
		trans.setMl_fraud_status(1);
		
		List<TransactionStatus> transList = new ArrayList<>();
		transList.add(trans);
		
		PCollection<TransactionStatus> transInput = pipeline.apply(Create.of(transList));
		PCollection<TransactionStatus> pubSubOutput = transInput.apply(new FilterGenuineTransactions());
	
		PAssert.that(pubSubOutput).empty();
		
		PipelineResult result = pipeline.run();
		System.out.println(result.getState());
		System.err.println(result.metrics());
		
	}
	
	
	/*
	 * Test filtering genuine transactions.
	 * Scenario 2 - 
	 * rule engine status = 1(genuine)
	 * ml model status = 0(genuine)
	 */
	@Test
	public void testFilteringTransactionsForGenuineScenario2() {
		
		Pipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
	
		TransactionStatus trans = new TransactionStatus();
		trans.setAmount(977.01);
		trans.setCc_number("3524574586339330");
		trans.setLatitude(26.888686);
		trans.setLongitude(-80.834389);
		trans.setRule_fraud_status(1);
		trans.setCategory("shopping_net");
		trans.setTrans_date_and_time("2020-06-21 01:00:08");
		trans.setMl_fraud_status(0);
		
		List<TransactionStatus> transList = new ArrayList<>();
		transList.add(trans);
		
		PCollection<TransactionStatus> transInput = pipeline.apply(Create.of(transList));
		PCollection<TransactionStatus> pubSubOutput = transInput.apply(new FilterGenuineTransactions());
	
		PAssert.that(pubSubOutput).containsInAnyOrder(trans);
		
		PipelineResult result = pipeline.run();
		System.out.println(result.getState());
		System.err.println(result.metrics());
		
	}
	
	
	/*
	 * Test filtering genuine transactions.
	 * Scenario 3 - 
	 * rule engine status = 0(fraud)
	 * ml model status = 1(fraud)
	 */
	@Test
	public void testFilteringTransactionsForGenuineScenario3() {
		
		Pipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
	
		TransactionStatus trans = new TransactionStatus();
		trans.setAmount(977.01);
		trans.setCc_number("3524574586339330");
		trans.setLatitude(26.888686);
		trans.setLongitude(-80.834389);
		trans.setRule_fraud_status(0);
		trans.setCategory("shopping_net");
		trans.setTrans_date_and_time("2020-06-21 01:00:08");
		trans.setMl_fraud_status(1);
		
		List<TransactionStatus> transList = new ArrayList<>();
		transList.add(trans);
		
		PCollection<TransactionStatus> transInput = pipeline.apply(Create.of(transList));
		PCollection<TransactionStatus> pubSubOutput = transInput.apply(new FilterGenuineTransactions());
	
		PAssert.that(pubSubOutput).empty();
		
		PipelineResult result = pipeline.run();
		System.out.println(result.getState());
		System.err.println(result.metrics());
		
	}
	
	
	/*
	 * Test filtering genuine transactions.
	 * Scenario 4 - 
	 * rule engine status = 0(fraud)
	 * ml model status = 0(genuine)
	 */
	@Test
	public void testFilteringTransactionsForGenuineScenario4() {
		
		Pipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
	
		TransactionStatus trans = new TransactionStatus();
		trans.setAmount(977.01);
		trans.setCc_number("3524574586339330");
		trans.setLatitude(26.888686);
		trans.setLongitude(-80.834389);
		trans.setRule_fraud_status(0);
		trans.setCategory("shopping_net");
		trans.setTrans_date_and_time("2020-06-21 01:00:08");
		trans.setMl_fraud_status(0);
		
		List<TransactionStatus> transList = new ArrayList<>();
		transList.add(trans);
		
		PCollection<TransactionStatus> transInput = pipeline.apply(Create.of(transList));
		PCollection<TransactionStatus> pubSubOutput = transInput.apply(new FilterGenuineTransactions());
	
		PAssert.that(pubSubOutput).empty();
		
		PipelineResult result = pipeline.run();
		System.out.println(result.getState());
		System.err.println(result.metrics());
		
	}
	//https://stackoverflow.com/questions/41850606/how-to-test-dataflow-pipeline-with-bigquery
	/*
	 * Test pipeline in actual environment.
	 */
	//@Test
	public void testPipeline() {
		GCPDataflowIntegration dataflow = new GCPDataflowIntegration();
		Pipeline pipeline = dataflow.getPipeline(false, GCP_PROJECT_ID, GCP_PROJECT_REGION);
		PipelineResult result = dataflow.createAndRunPipeline(pipeline, GCP_PROJECT_ID, TRANSACTION_TOPIC, DATASET_ID, TABLE_ID, FRAUD_STATUS_TOPIC, TRANSACTION_STATUS_TOPIC);
		assertEquals("RUNNING", result.getState().toString());
	}
	
	
	/*
	 * Test pipeline in test environment.
	 */
	//@Test
	public void testPipelineInTestEnviorment() {
		GCPDataflowIntegration dataflow = new GCPDataflowIntegration();
		Pipeline pipeline = dataflow.getPipeline(true, GCP_PROJECT_ID, GCP_PROJECT_REGION);
		PipelineResult result = dataflow.createAndRunPipeline(pipeline, GCP_PROJECT_ID, TRANSACTION_TOPIC, DATASET_ID, TABLE_ID, FRAUD_STATUS_TOPIC, TRANSACTION_STATUS_TOPIC);
		assertEquals("DONE", result.getState());
	}
	
	
	/*
	 * Test pipeline locally.
	 * Scenario 1 - when transaction is fraud.
	 * 
	 * rule engine status = 0(fraud)
	 * ml model status = 1(fraud)
	 * 
	 * Filter fraud transactions list has an object.
	 * Filter genuine transactions list is empty.
	 */
	@Test
	public void testEndToEndPipelineScenario1() {
		
		messageWithCategory = "{\r\n"
				+ "    \"latitude\": 26.888686,\r\n"
				+ "	\"longitude\": -80.834389,\r\n"
				+ "    \"amount\": 977.01,\r\n"
				+ "    \"dateTime\": \"2020-06-21 01:00:08\",\r\n"
				+ "    \"category\": \"shopping_net\",\r\n"
				+ "	\"cardNo\" : \"3524574586339330\",\r\n"
				+ "	\r\n"
				+ "}";
		
		Pipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
		
		byte[] data = messageWithCategory.getBytes();
		PubsubMessage pubsubMessage = new PubsubMessage(data, null);
		List<PubsubMessage> msgList = new ArrayList<>();
		msgList.add(pubsubMessage);
		
		PCollection<PubsubMessage> pubSubInput = pipeline.apply(Create.of(msgList));
		PCollection<Transaction> pubSubOutput = pubSubInput.apply(new ReadPubSubMessages());
		
		PCollection<TransactionStatus> ruleEngineOutput = pubSubOutput.apply(new InvokeRuleEngine());
		PCollection<TransactionStatus> mlModelOutput = ruleEngineOutput.apply(new InvokeMLModel());
	
		PCollection<TransactionStatus> filteredFraudTransactions = mlModelOutput.apply(new FilterFraudTransactions());
		
		PCollection<TransactionStatus> filteredGenuineTransactions = mlModelOutput.apply(new FilterGenuineTransactions());
		
		TransactionStatus trans = new TransactionStatus();
		trans.setAmount(977.01);
		trans.setCc_number("3524574586339330");
		trans.setLatitude(26.888686);
		trans.setLongitude(-80.834389);
		trans.setRule_fraud_status(0);
		trans.setCategory("shopping_net");
		trans.setTrans_date_and_time("2020-06-21 01:00:08");
		trans.setMl_fraud_status(1);
		
		PAssert.that(filteredFraudTransactions).containsInAnyOrder(trans);
		PAssert.that(filteredGenuineTransactions).empty();
		
		PipelineResult result = pipeline.run();
		System.out.println(result.getState());
		System.err.println(result.metrics());
		
	}
	
	
	/*
	 * Test pipeline locally.
	 * Scenario 2 - when transaction is fraud.
	 * 
	 * rule engine status = 0(fraud)
	 * ml model status = 0(genuine)
	 * 
	 * Filter fraud transactions list has an object.
	 * Filter genuine transactions list is empty.
	 */
	@Test
	public void testEndToEndPipelineScenario2() {
		
		messageWithCategory = "{\r\n"
				+ "    \"latitude\": 26.888686,\r\n"
				+ "	\"longitude\": -80.834389,\r\n"
				+ "    \"amount\": 501,\r\n"
				+ "    \"dateTime\": \"2020-06-21 01:00:08\",\r\n"
				+ "    \"category\": \"shopping_net\",\r\n"
				+ "	\"cardNo\" : \"3524574586339330\",\r\n"
				+ "	\r\n"
				+ "}";
		
		Pipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
		
		byte[] data = messageWithCategory.getBytes();
		PubsubMessage pubsubMessage = new PubsubMessage(data, null);
		List<PubsubMessage> msgList = new ArrayList<>();
		msgList.add(pubsubMessage);
		
		PCollection<PubsubMessage> pubSubInput = pipeline.apply(Create.of(msgList));
		PCollection<Transaction> pubSubOutput = pubSubInput.apply(new ReadPubSubMessages());
		
		PCollection<TransactionStatus> ruleEngineOutput = pubSubOutput.apply(new InvokeRuleEngine());
		PCollection<TransactionStatus> mlModelOutput = ruleEngineOutput.apply(new InvokeMLModel());
	
		PCollection<TransactionStatus> filteredFraudTransactions = mlModelOutput.apply(new FilterFraudTransactions());
		
		PCollection<TransactionStatus> filteredGenuineTransactions = mlModelOutput.apply(new FilterGenuineTransactions());
		
		TransactionStatus trans = new TransactionStatus();
		trans.setAmount(501);
		trans.setCc_number("3524574586339330");
		trans.setLatitude(26.888686);
		trans.setLongitude(-80.834389);
		trans.setRule_fraud_status(0);
		trans.setCategory("shopping_net");
		trans.setTrans_date_and_time("2020-06-21 01:00:08");
		trans.setMl_fraud_status(0);
		
		PAssert.that(filteredFraudTransactions).containsInAnyOrder(trans);
		PAssert.that(filteredGenuineTransactions).empty();
		
		PipelineResult result = pipeline.run();
		System.out.println(result.getState());
		System.err.println(result.metrics());
		
	}
	
	
	/*
	 * Test pipeline locally.
	 * Scenario 3 - when transaction is genuine.
	 * 
	 * rule engine status = 1(genuine)
	 * ml model status = 0(genuine)
	 * 
	 * Filter fraud transactions list is empty.
	 * Filter genuine transactions list has an object.
	 */
	@Test
	public void testEndToEndPipelineScenario3() {
		
		messageWithCategory = "{\r\n"
				+ "    \"latitude\": 26.888686,\r\n"
				+ "	\"longitude\": -80.834389,\r\n"
				+ "    \"amount\": 9.16,\r\n"
				+ "    \"dateTime\": \"2020-06-21 01:00:08\",\r\n"
				+ "    \"category\": \"shopping_net\",\r\n"
				+ "	\"cardNo\" : \"3524574586339330\",\r\n"
				+ "	\r\n"
				+ "}";
		
		Pipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
		
		byte[] data = messageWithCategory.getBytes();
		PubsubMessage pubsubMessage = new PubsubMessage(data, null);
		List<PubsubMessage> msgList = new ArrayList<>();
		msgList.add(pubsubMessage);
		
		PCollection<PubsubMessage> pubSubInput = pipeline.apply(Create.of(msgList));
		PCollection<Transaction> pubSubOutput = pubSubInput.apply(new ReadPubSubMessages());
		
		PCollection<TransactionStatus> ruleEngineOutput = pubSubOutput.apply(new InvokeRuleEngine());
		PCollection<TransactionStatus> mlModelOutput = ruleEngineOutput.apply(new InvokeMLModel());
	
		PCollection<TransactionStatus> filteredFraudTransactions = mlModelOutput.apply(new FilterFraudTransactions());
		
		PCollection<TransactionStatus> filteredGenuineTransactions = mlModelOutput.apply(new FilterGenuineTransactions());
		
		TransactionStatus trans = new TransactionStatus();
		trans.setAmount(9.16);
		trans.setCc_number("3524574586339330");
		trans.setLatitude(26.888686);
		trans.setLongitude(-80.834389);
		trans.setRule_fraud_status(1);
		trans.setCategory("shopping_net");
		trans.setTrans_date_and_time("2020-06-21 01:00:08");
		trans.setMl_fraud_status(0);
		
		PAssert.that(filteredGenuineTransactions).containsInAnyOrder(trans);
		PAssert.that(filteredFraudTransactions).empty();
		
		PipelineResult result = pipeline.run();
		System.out.println(result.getState());
		System.err.println(result.metrics());
		
	}
	
	
	/*
	 * Test pipeline locally.
	 * Scenario 4 - when transaction is fraud.
	 * 
	 * rule engine status = 1(genuine)
	 * ml model status = 1(fraud)
	 * 
	 * Filter fraud transactions list has an object.
	 * Filter genuine transactions list is empty.
	 */
	@Test
	public void testEndToEndPipelineScenario4() {
		
		messageWithCategory = "{\r\n"
				+ "    \"latitude\": 32.675272,\r\n"
				+ "	\"longitude\": -103.484949,\r\n"
				+ "    \"amount\": 21.69,\r\n"
				+ "    \"dateTime\": \"2020-06-21 03:59:46\",\r\n"
				+ "    \"category\": \"gas_transport\",\r\n"
				+ "	\"cardNo\" : \"3560725013359370\",\r\n"
				+ "	\r\n"
				+ "}";
		
		Pipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
		
		byte[] data = messageWithCategory.getBytes();
		PubsubMessage pubsubMessage = new PubsubMessage(data, null);
		List<PubsubMessage> msgList = new ArrayList<>();
		msgList.add(pubsubMessage);
		
		PCollection<PubsubMessage> pubSubInput = pipeline.apply(Create.of(msgList));
		PCollection<Transaction> pubSubOutput = pubSubInput.apply(new ReadPubSubMessages());
		
		PCollection<TransactionStatus> ruleEngineOutput = pubSubOutput.apply(new InvokeRuleEngine());
		PCollection<TransactionStatus> mlModelOutput = ruleEngineOutput.apply(new InvokeMLModel());
	
		PCollection<TransactionStatus> filteredFraudTransactions = mlModelOutput.apply(new FilterFraudTransactions());
		
		PCollection<TransactionStatus> filteredGenuineTransactions = mlModelOutput.apply(new FilterGenuineTransactions());
		
		TransactionStatus trans = new TransactionStatus();
		trans.setAmount(21.69);
		trans.setCc_number("3560725013359370");
		trans.setLatitude(32.675272);
		trans.setLongitude(-103.484949);
		trans.setRule_fraud_status(1);
		trans.setCategory("gas_transport");
		trans.setTrans_date_and_time("2020-06-21 03:59:46");
		trans.setMl_fraud_status(1);
		
		PAssert.that(filteredFraudTransactions).containsInAnyOrder(trans);
		PAssert.that(filteredGenuineTransactions).empty();
		
		PipelineResult result = pipeline.run();
		System.out.println(result.getState());
		System.err.println(result.metrics());
		
	}

}
