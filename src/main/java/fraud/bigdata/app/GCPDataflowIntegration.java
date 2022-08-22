package fraud.bigdata.app;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

import com.google.api.services.bigquery.model.TableRow;

import fraud.bigdata.app.entity.Transaction;
import fraud.bigdata.app.entity.TransactionStatus;

public class GCPDataflowIntegration {

	// https://cloud.google.com/dataflow/docs/guides/setting-pipeline-options#java_4
	public Pipeline getPipeline(boolean isTest, String projectId, String region) {
		if (isTest) {
			PipelineOptions options = PipelineOptionsFactory.create();
			Pipeline pipeline = Pipeline.create(options);
			return pipeline;
		}
		DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
		options.setProject(projectId);
		options.setRegion(region);
		options.setRunner(DataflowRunner.class);

		Pipeline pipeline = Pipeline.create(options);
		return pipeline;
	}

	public PipelineResult createAndRunPipeline(Pipeline pipeline, String project_id, String transaction_topic,
			String datasetId, String tableId, String fraud_status_topic, String transaction_status_topic) {
		
		PCollection<PubsubMessage> pubSubInput = pipeline.apply(PubsubIO.readMessages().fromTopic(transaction_topic));
		PCollection<Transaction> pubSubOutput = pubSubInput.apply(new ReadPubSubMessages());

		PCollection<TransactionStatus> ruleEngineOutput = pubSubOutput.apply(new InvokeRuleEngine());
		PCollection<TransactionStatus> mlModelOutput = ruleEngineOutput.apply(new InvokeMLModel());

		PCollection<TableRow> bigQueryOutput = mlModelOutput.apply(new WriteToBigQuery());
		bigQueryOutput.apply(BigQueryIO.writeTableRows().to(project_id + ":" + datasetId + "." + tableId)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

		PCollection<TransactionStatus> filteredTransactions = mlModelOutput.apply(new FilterFraudTransactions());
		PCollection<String> fraudPubSubWriteOutPut = filteredTransactions.apply(new WriteToPubSub());

		PCollection<TransactionStatus> genuineTransactions = mlModelOutput.apply(new FilterGenuineTransactions());
		PCollection<String> genuinePubSubWriteOutPut = genuineTransactions.apply(new WriteToPubSub());

		fraudPubSubWriteOutPut.apply(PubsubIO.writeStrings().to(fraud_status_topic));

		genuinePubSubWriteOutPut.apply(PubsubIO.writeStrings().to(transaction_status_topic));
		
		return pipeline.run();
	}

}
