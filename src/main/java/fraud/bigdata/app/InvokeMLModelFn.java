package fraud.bigdata.app;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;

import com.google.cloud.automl.v1beta1.AnnotationPayload;
import com.google.cloud.automl.v1beta1.ExamplePayload;
import com.google.cloud.automl.v1beta1.ModelName;
import com.google.cloud.automl.v1beta1.PredictRequest;
import com.google.cloud.automl.v1beta1.PredictResponse;
import com.google.cloud.automl.v1beta1.PredictionServiceClient;
import com.google.cloud.automl.v1beta1.PredictionServiceSettings;
import com.google.cloud.automl.v1beta1.Row;
import com.google.cloud.automl.v1beta1.TablesAnnotation;
import com.google.protobuf.Value;

import fraud.bigdata.app.entity.TransactionStatus;

public class InvokeMLModelFn extends DoFn<TransactionStatus, TransactionStatus> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@ProcessElement
	public void processElement(ProcessContext c) throws IOException {

		String projectId = "ultra-palisade-339421";
		String modelId = "TBL3276038875423703040";
		List<Value>  values = new ArrayList<>();
		values.add(Value.newBuilder().setStringValue(c.element().getTrans_date_and_time()).build());
		values.add(Value.newBuilder().setStringValue(c.element().getCc_number()).build());
		values.add(Value.newBuilder().setStringValue(c.element().getCategory()).build());
		values.add(Value.newBuilder().setNumberValue(c.element().getAmount()).build());
		values.add(Value.newBuilder().setNumberValue(c.element().getLatitude()).build());
		values.add(Value.newBuilder().setNumberValue(c.element().getLongitude()).build());
		
		PredictionServiceSettings settings = PredictionServiceSettings.newBuilder()
		          .setEndpoint("eu-automl.googleapis.com:443").build();
		try (PredictionServiceClient client = PredictionServiceClient.create(settings)) {
			// Get the full path of the model.
			ModelName name = ModelName.of(projectId, "eu", modelId);
			Row row = Row.newBuilder().addAllValues(values).build();
			ExamplePayload payload = ExamplePayload.newBuilder().setRow(row).build();
			//System.out.println("Prediction results:" + payload.toString());
			// Feature importance gives you visibility into how the features in a specific
			// prediction
			// request informed the resulting prediction. For more info, see:
			// https://cloud.google.com/automl-tables/docs/features#local
			PredictRequest request = PredictRequest.newBuilder().setName(name.toString()).setPayload(payload)
					.putParams("feature_importance", "false").build();

			PredictResponse response = client.predict(request);

			System.out.println("Prediction results:");
			double score = 0;
			int isFraud = 0;
			for (AnnotationPayload annotationPayload : response.getPayloadList()) {
				TablesAnnotation tablesAnnotation = annotationPayload.getTables();
				System.out.format("Classification label: %s%n", tablesAnnotation.getValue().getStringValue());
				System.out.format("Classification score: %.3f%n", tablesAnnotation.getScore());
				if(tablesAnnotation.getScore() > score) {
					if(tablesAnnotation.getValue().getStringValue().toLowerCase().equals("1")) {
						isFraud = 1;
					}
					score = tablesAnnotation.getScore();
				}

				// Get features of top importance
				/*tablesAnnotation.getTablesModelColumnInfoList()
						.forEach(info -> System.out.format("\tColumn: %s - Importance: %.2f%n",
								info.getColumnDisplayName(), info.getFeatureImportance()));*/
			}
			System.out.format("Classification label of the record: %s%n", isFraud);
			System.out.format("Classification score of the record : %.3f%n ", score);
			
			TransactionStatus mlStausObj = new TransactionStatus();
			mlStausObj.setMl_fraud_status(isFraud);
			mlStausObj.setAmount(c.element().getAmount());
			mlStausObj.setCategory(c.element().getCategory());
			mlStausObj.setCc_number(c.element().getCc_number());
			mlStausObj.setLatitude(c.element().getLatitude());
			mlStausObj.setLongitude(c.element().getLongitude());
			mlStausObj.setRule_fraud_status(c.element().getRule_fraud_status());
			mlStausObj.setTrans_date_and_time(c.element().getTrans_date_and_time());
			
			c.output(mlStausObj);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
