package fraud.bigdata.app;

import java.nio.charset.Charset;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.json.JSONObject;

import fraud.bigdata.app.entity.Transaction;

public class ReadPubSubMessagesFn extends DoFn<PubsubMessage, Transaction> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	@ProcessElement
    public void processElement(ProcessContext c) {

		String record = new String(c.element().getPayload(), Charset.forName("UTF-8"));
		System.out.println("Payload : "+ record);
        
        JSONObject jsonObject = new JSONObject(record);
        
        double latitude = jsonObject.getDouble("latitude");
        double longitude = jsonObject.getDouble("longitude");
        double amount = jsonObject.getDouble("amount");
        String dateTime = jsonObject.getString("dateTime");
        String cardNo = jsonObject.getString("cardNo");
        String category = jsonObject.getString("category");
        
        System.out.println("Latitude :"+ latitude);
        System.out.println("Longitude : "+ longitude);
        System.out.println("Amount : "+ amount);
        System.out.println("DateTime : "+ dateTime);
        System.out.println("cardNo : "+ cardNo);
        System.out.println("category : "+ category);
        
        Transaction trans = new Transaction();
        trans.setAmount(amount);
        trans.setCardNo(cardNo);
        trans.setDateTime(dateTime);
        trans.setLatitude(latitude);
        trans.setLongitude(longitude);
        trans.setCategory(category);
        
        c.output(trans);
    }

}
