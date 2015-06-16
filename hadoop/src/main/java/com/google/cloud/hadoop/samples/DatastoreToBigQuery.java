package com.google.cloud.hadoop.samples;

import static com.google.api.services.datastore.client.DatastoreHelper.KEY_PROPERTY_NAME;
import static com.google.api.services.datastore.client.DatastoreHelper.makeFilter;
import static com.google.api.services.datastore.client.DatastoreHelper.makeKey;
import static com.google.api.services.datastore.client.DatastoreHelper.makeValue;

import com.google.api.services.datastore.DatastoreV1.KindExpression;
import com.google.api.services.datastore.DatastoreV1.Property;
import com.google.api.services.datastore.DatastoreV1.PropertyFilter;
import com.google.api.services.datastore.DatastoreV1.Query;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryOutputFormat;
import com.google.cloud.hadoop.io.datastore.DatastoreEntity;
import com.google.cloud.hadoop.io.datastore.DatastoreHadoopInputFormat;
import com.google.cloud.hadoop.io.datastore.DatastoreKey;
import com.google.gson.JsonObject;
import com.google.protobuf.TextFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Sample program to run the Hadoop Wordcount example reading from Datastore and exporting to
 * BigQuery.
 */
public class DatastoreToBigQuery {
  /**
   * The mapper function for word count takes a Datastore Entity.
   */
  public static class Map extends Mapper<DatastoreKey, DatastoreEntity, Text, IntWritable> {
    @Override
    public void map(DatastoreKey key, DatastoreEntity value, Context context) throws IOException,
        InterruptedException {
      // Iterate over Entity properties.
      for (Property prop : value.get().getPropertyList()) {
        // If Entity has a property line.
        if (prop.getName().equals("line")) {
          // Split line into words.
          String line = prop.getValue().getStringValue();
          String[] tokenizer = line.split(" ");
          for (String token : tokenizer) {
            Text word = new Text();
            word.set(token.replaceAll("[^A-Za-z]", "").toLowerCase());
            // Output each word and a count of 1.
            context.write(word, new IntWritable(1));
          }
        }
      }
    }
  }

  /**
   * Reducer function for word count writes a JsonObject that represents a BigQuery object.
   */
  public static class Reduce extends Reducer<Text, IntWritable, Text, JsonObject> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
        InterruptedException {
      // Get total count for word.
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      // If word is not empty.
      if (!key.toString().isEmpty()) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("Word", key.toString());
        jsonObject.addProperty("Number", sum);
        // Key does not matter.
        context.write(new Text("ignored"), jsonObject);
      }
    }
  }

  // Print a usage statement and exit with an error code (1).
  private static void printUsageAndExit() {
    System.out.println(
        "Usage: hadoop jar datastoretobigquery_wordcount.jar [datasetId] [projectId] "
            + " [outputDatasetId] [outputTableId] [inputKindName] [jobName].  "
            + "Please enter all parameters");
    System.exit(1);
  }

  /**
   * Configures and runs a WordCount job reading from the Cloud Datastore and writing to BigQuery.
   *
   * @param args a String[] containing your datasetId, your projectId, your outputDatasetId, and
   *        your outputTableId.
   */
  public static void main(String[] args) throws Exception {

    GenericOptionsParser parser = new GenericOptionsParser(args);
    args = parser.getRemainingArgs();

    // Check all args entered.
    if (args.length != 6) {
      printUsageAndExit();
    }

    // Set parameters from args.
    String datastoreDatasetId = args[0];
    String projectId = args[1];
    String outputDatasetId = args[2];
    String outputTableId = args[3];
    String inputKindName = args[4];
    String jobName = args[5];

    // Check that projectId, output dataset and output table are not empty.
    if ("".equals(projectId) || "".equals(outputDatasetId) || "".equals(outputTableId)) {
      printUsageAndExit();
    }

    // Set default parameters for this program
    String fields = "[{'name': 'Word','type': 'STRING'},{'name': 'Number','type': 'INTEGER'}]";

    // Configure Map Reduce for WordCount job.
    JobConf conf = new JobConf(parser.getConfiguration(), DatastoreToBigQuery.class);
    BigQueryConfiguration.configureBigQueryOutput(conf,
        projectId,
        outputDatasetId,
        outputTableId,
        fields);
    Job job = new Job(conf, jobName);
    job.setJarByClass(DatastoreToBigQuery.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    // Set input and output classes.
    job.setInputFormatClass(DatastoreHadoopInputFormat.class);
    job.setOutputFormatClass(BigQueryOutputFormat.class);

    // Set WordCount query.
    Query.Builder q = Query.newBuilder();
    KindExpression.Builder kind = KindExpression.newBuilder();
    kind.setName(inputKindName);
    q.addKind(kind);
    q.setFilter(makeFilter(KEY_PROPERTY_NAME, PropertyFilter.Operator.HAS_ANCESTOR,
        makeValue(makeKey(inputKindName, WordCountSetUp.ANCESTOR_ENTITY_VALUE))));
    String query = TextFormat.printToString(q);

    // Set parameters for DatastoreHadoopInputFormat.
    DatastoreHadoopInputFormat.setInput(job, query, datastoreDatasetId);

    job.waitForCompletion(true);
  }
}
