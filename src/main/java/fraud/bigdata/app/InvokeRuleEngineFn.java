package fraud.bigdata.app;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import org.apache.beam.sdk.transforms.DoFn;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import fraud.bigdata.app.entity.Transaction;
import fraud.bigdata.app.entity.TransactionStatus;

public class InvokeRuleEngineFn extends DoFn<Transaction, TransactionStatus>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	
	@ProcessElement
    public void processElement(ProcessContext c) {
		
		ObjectMapper objectMapper = new ObjectMapper();
		String requestBody = null;
		HttpResponse<String> response = null;
		
		try {
			requestBody = objectMapper.writeValueAsString(c.element());
			System.out.println("Request body : " + requestBody);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		HttpClient client = HttpClient.newHttpClient();
		
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://34.89.222.32:8080/gcp-fraud-detection/api/isTransactionValid"))
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .header("Content-Type", "application/json")
                .build();

        try {
			response = client.send(request,
			        HttpResponse.BodyHandlers.ofString());
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
        
        String responseBody = response.body();
		System.out.println("Response : " +responseBody);
		int responseCode = 0;
		
		if(responseBody.toLowerCase().equals("true")) {
			responseCode = 1;
		}
		
		TransactionStatus ruleStatusObj = new TransactionStatus();
		ruleStatusObj.setAmount(c.element().getAmount());
		ruleStatusObj.setCategory(c.element().getCategory());
		ruleStatusObj.setCc_number(c.element().getCardNo());
		ruleStatusObj.setLatitude(c.element().getLatitude());
		ruleStatusObj.setLongitude(c.element().getLongitude());
		ruleStatusObj.setRule_fraud_status(responseCode);
		ruleStatusObj.setMl_fraud_status(0);
		ruleStatusObj.setTrans_date_and_time(c.element().getDateTime());
		
        c.output(ruleStatusObj);
	}
}
