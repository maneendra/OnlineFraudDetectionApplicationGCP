package fraud.bigdata.app;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.vendor.grpc.v1p43p2.com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

public class GCPBigQueryIntegrationTest {

	private Pipeline pipeline;
	private PipelineOptions options;
	private static final String PROJECT_ID = "project-id";
	private static final String DATASET_ID = "dataset-id";
	private static final String TABLE_ID = "table-id";

	private Table table;
	private TableReference tableRef;

	private FakeDatasetService fakeDatasetService = new FakeDatasetService();
	private FakeJobService fakeJobService = new FakeJobService();
	private FakeBigQueryServices fakeBqServices = new FakeBigQueryServices().withDatasetService(fakeDatasetService)
			.withJobService(fakeJobService);

	@Before
	public void setUp() throws IOException, InterruptedException {
		options = TestPipeline.testingPipelineOptions();
		BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
		bqOptions.setProject(PROJECT_ID);

		bqOptions.setTempLocation("/fraud.bigdata.app/src/test/resources/staging_location");
		pipeline = TestPipeline.fromOptions(options).enableAbandonedNodeEnforcement(false);

		FakeDatasetService.setUp();

		fakeDatasetService.createDataset(PROJECT_ID, DATASET_ID, "", "", null);

	}

	/*
	 * Test the exception when used CreateDisposition.CREATE_NEVER during
	 * BigQueryIO.Write and there is no table in Big Query
	 */
	@Test(expected = PipelineExecutionException.class)
	public void testBigQueryWriteExceptionWhenCreateDispositionIsCreateNeverAndTableIsNotPresent()
			throws InterruptedException, IOException {
		TableRow row1 = new TableRow().set("name", "a").set("number", 1);
		TableRow row2 = new TableRow().set("name", "b").set("number", 2);
		TableRow row3 = new TableRow().set("name", "c").set("number", 3);

		List<TableRow> rows = new ArrayList<TableRow>();
		rows.add(row1);
		rows.add(row2);
		rows.add(row3);
		
		GCPBigQueryIntegration bqIntegration = new GCPBigQueryIntegration();
		bqIntegration.writeRowsToBigQuery(true, pipeline, rows, DATASET_ID, TABLE_ID, fakeBqServices);

		pipeline.run();

	}

	/*
	 * Test CreateDisposition.CREATE_NEVER during BigQueryIO.Write and there is
	 * already a table in Big Query
	 */
	@Test
	public void testBigQueryWriteWhenCreateDispositionIsCreateNeverAndTableIsPresent()
			throws InterruptedException, IOException {
		table = new Table();
		tableRef = new TableReference();
		tableRef.setDatasetId("dataset-id");
		tableRef.setProjectId("project-id");
		tableRef.setTableId("table-id");
		table.setTableReference(tableRef);

		table.setSchema(
				new TableSchema().setFields(ImmutableList.of(new TableFieldSchema().setName("name").setType("STRING"),
						new TableFieldSchema().setName("number").setType("INTEGER"))));
		fakeDatasetService.createTable(table);
		
		TableRow row1 = new TableRow().set("name", "a").set("number", 1);
		TableRow row2 = new TableRow().set("name", "b").set("number", 2);
		TableRow row3 = new TableRow().set("name", "c").set("number", 3);

		List<TableRow> rows = new ArrayList<TableRow>();
		rows.add(row1);
		rows.add(row2);
		rows.add(row3);
		
		GCPBigQueryIntegration bqIntegration = new GCPBigQueryIntegration();
		WriteResult result = bqIntegration.writeRowsToBigQuery(true, pipeline, rows, DATASET_ID, TABLE_ID, fakeBqServices);

		PAssert.that(result.getSuccessfulTableLoads())
				.containsInAnyOrder(new TableDestination("project-id:dataset-id.table-id", null));

		pipeline.run();
		
		fakeDatasetService.deleteTable(tableRef);
	}

	/*
	 * Test CreateDisposition.WRITE_APPEND during BigQueryIO.Write and there is
	 * already a table in Big Query
	 */
	@Test
	public void testBigQueryWriteWhenWriteDispositionIsWriteAppendAndTableIsPresent()
			throws InterruptedException, IOException {
		table = new Table();
		tableRef = new TableReference();
		tableRef.setDatasetId("dataset-id");
		tableRef.setProjectId("project-id");
		tableRef.setTableId("table-id");
		table.setTableReference(tableRef);

		table.setSchema(
				new TableSchema().setFields(ImmutableList.of(new TableFieldSchema().setName("name").setType("STRING"),
						new TableFieldSchema().setName("number").setType("INTEGER"))));
		fakeDatasetService.createTable(table);

		TableRow row1 = new TableRow().set("name", "a").set("number", 1);
		TableRow row2 = new TableRow().set("name", "b").set("number", 2);
		TableRow row3 = new TableRow().set("name", "c").set("number", 3);

		List<TableRow> rows = new ArrayList<TableRow>();
		rows.add(row1);
		rows.add(row2);
		rows.add(row3);
		
		GCPBigQueryIntegration bqIntegration = new GCPBigQueryIntegration();
		bqIntegration.writeRowsToBigQuery(true, pipeline, rows, DATASET_ID, TABLE_ID, fakeBqServices);

		pipeline.run();

		List<TableRow> inserts = fakeDatasetService.getAllRows("project-id", "dataset-id", "table-id");
		assertEquals(3, inserts.size());
		
		fakeDatasetService.deleteTable(tableRef);

	}

	@After
	public void tearDown() throws IOException, InterruptedException {
		
	}
}
