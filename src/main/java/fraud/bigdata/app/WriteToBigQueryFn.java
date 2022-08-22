package fraud.bigdata.app;

import org.apache.beam.sdk.transforms.DoFn;

import com.google.api.services.bigquery.model.TableRow;

import fraud.bigdata.app.entity.TransactionStatus;

public class WriteToBigQueryFn extends   DoFn<TransactionStatus, TableRow>{
	
	private static final long serialVersionUID = 1L;
	@ProcessElement
    public void processElement(ProcessContext c) {
		TableRow row1 = new TableRow();
		row1.set("amount", c.element().getAmount());
		row1.set("category", c.element().getCategory());
		row1.set("longitude", c.element().getLongitude());
		row1.set("latitude", c.element().getLatitude());
		row1.set("cc_number", c.element().getCc_number());
		row1.set("trans_date_and_time", c.element().getTrans_date_and_time());
		row1.set("ml_fraud_status", c.element().getMl_fraud_status());
		row1.set("rule_fraud_status", c.element().getRule_fraud_status());
		c.output(row1);
	}

}
