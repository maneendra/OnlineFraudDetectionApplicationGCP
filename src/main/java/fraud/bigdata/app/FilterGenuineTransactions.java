package fraud.bigdata.app;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import fraud.bigdata.app.entity.TransactionStatus;

public class FilterGenuineTransactions extends PTransform<PCollection<TransactionStatus>, PCollection<TransactionStatus>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public PCollection<TransactionStatus> expand(PCollection<TransactionStatus> input) {
		return input.apply(ParDo.of(new FilterGenuineTransactionsFn()));
	}



}
