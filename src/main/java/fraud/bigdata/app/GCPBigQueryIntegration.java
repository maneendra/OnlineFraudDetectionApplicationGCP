package fraud.bigdata.app;

import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.transforms.Create;

import com.google.api.services.bigquery.model.TableRow;

public class GCPBigQueryIntegration {

	public WriteResult writeRowsToBigQuery(boolean isTest, Pipeline pipeline, List<TableRow> rows, String datasetId,
			String tableId, FakeBigQueryServices fakeBqServices) {
		WriteResult result ;
		if (isTest) {
			result = pipeline.apply(Create.of(rows))
					.apply(BigQueryIO.writeTableRows().to(datasetId + "." + tableId)
							.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
							.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
							.withTestServices(fakeBqServices).withoutValidation());
		}

		else {
			result = pipeline.apply(Create.of(rows))
					.apply(BigQueryIO.writeTableRows().to(datasetId + "." + tableId)
							.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
							.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
		}
		return result;
	}

}
