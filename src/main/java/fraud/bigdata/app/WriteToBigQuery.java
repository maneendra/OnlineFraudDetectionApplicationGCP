package fraud.bigdata.app;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import com.google.api.services.bigquery.model.TableRow;

import fraud.bigdata.app.entity.TransactionStatus;

public class WriteToBigQuery extends PTransform<PCollection<TransactionStatus>, PCollection<TableRow>> {

	private static final long serialVersionUID = 1L;

	@Override
	public PCollection<TableRow> expand(PCollection<TransactionStatus> input) {
		return input.apply(ParDo.of(new WriteToBigQueryFn()));
	}

}
