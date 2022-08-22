package fraud.bigdata.app;

import org.apache.beam.sdk.transforms.DoFn;

import fraud.bigdata.app.entity.TransactionStatus;

public class FilterGenuineTransactionsFn extends DoFn<TransactionStatus, TransactionStatus> {
	
	private static final long serialVersionUID = 1L;
	@ProcessElement
    public void processElement(ProcessContext c) {
		if(c.element().getMl_fraud_status() == 0 && c.element().getRule_fraud_status() == 1) {
			c.output(c.element());
		}
		
	}

}
