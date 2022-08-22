package fraud.bigdata.app;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import fraud.bigdata.app.entity.Transaction;

public class ReadPubSubMessages extends PTransform<PCollection<PubsubMessage>, PCollection<Transaction>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public PCollection<Transaction> expand(PCollection<PubsubMessage> input) {
		return input.apply(ParDo.of(new ReadPubSubMessagesFn()));
	}

}
