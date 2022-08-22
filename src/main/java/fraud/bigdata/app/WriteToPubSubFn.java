package fraud.bigdata.app;

import org.apache.beam.sdk.transforms.DoFn;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import fraud.bigdata.app.entity.TransactionStatus;

public class WriteToPubSubFn extends DoFn<TransactionStatus, String> {
	
	private static final long serialVersionUID = 1L;
	
	@ProcessElement
    public void processElement(ProcessContext c) {
		ObjectMapper objectMapper = new ObjectMapper();
		String jsonString = null;
		
		try {
			jsonString = objectMapper.writeValueAsString(c.element());
			System.out.println("Converted JSON string  : " + jsonString);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		
		c.output(jsonString);
	}

}
