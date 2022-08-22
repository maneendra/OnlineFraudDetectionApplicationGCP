package fraud.bigdata.app;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import fraud.bigdata.app.entity.Transaction;
import fraud.bigdata.app.entity.TransactionStatus;

public class InvokeRuleEngine extends PTransform<PCollection<Transaction>, PCollection<TransactionStatus>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public PCollection<TransactionStatus> expand(PCollection<Transaction> input) {
		return input.apply(ParDo.of(new InvokeRuleEngineFn()));
	}

}
