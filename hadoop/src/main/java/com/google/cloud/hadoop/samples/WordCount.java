package com.google.cloud.hadoop.samples;

import static com.google.api.services.datastore.client.DatastoreHelper.KEY_PROPERTY_NAME;
import static com.google.api.services.datastore.client.DatastoreHelper.makeFilter;
import static com.google.api.services.datastore.client.DatastoreHelper.makeKey;
import static com.google.api.services.datastore.client.DatastoreHelper.makeProperty;
import static com.google.api.services.datastore.client.DatastoreHelper.makeValue;

import com.google.api.services.datastore.DatastoreV1.Entity;
import com.google.api.services.datastore.DatastoreV1.Key;
import com.google.api.services.datastore.DatastoreV1.Key.PathElement;
import com.google.api.services.datastore.DatastoreV1.KindExpression;
import com.google.api.services.datastore.DatastoreV1.Property;
import com.google.api.services.datastore.DatastoreV1.PropertyFilter;
import com.google.api.services.datastore.DatastoreV1.Query;
import com.google.cloud.hadoop.io.datastore.DatastoreEntity;
import com.google.cloud.hadoop.io.datastore.DatastoreHadoopInputFormat;
import com.google.cloud.hadoop.io.datastore.DatastoreHadoopOutputFormat;
import com.google.cloud.hadoop.io.datastore.DatastoreKey;
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
 * Sample program to run the Hadoop Wordcount example
 */
public class WordCount {
  // The kind of output entities for wordcount.
  static final String OUTPUT_KIND_NAME = "mapred.datastore.samples.output.kind";

  /**
   * The mapper function for word count.
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
   * Reducer function for word count.
   */
  public static class Reduce extends Reducer<Text, IntWritable, DatastoreKey, DatastoreEntity> {
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
        // Create Entity for each word containing the count and the word.
        PathElement.Builder p = PathElement.newBuilder();
        p.setKind(context.getConfiguration().get(OUTPUT_KIND_NAME));
        p.setName(key.toString());
        Key.Builder k = Key.newBuilder();
        k.addPathElement(p);
        Entity.Builder e = Entity.newBuilder();
        e.addProperty(makeProperty("count", makeValue(sum)));
        e.addProperty(makeProperty("word", makeValue(key.toString())));
        e.setKey(k);
        // Write Entity to output.
        context.write(new DatastoreKey(), new DatastoreEntity(e.build()));
      }
    }
  }

  // Print a usage statement and exit.
  private static void printUsageAndExit() {
    System.out.print(
        "Usage: hadoop jar datastore_wordcount.jar [datasetId] [inputKindName] [outputKindName]"
            + " [jobName].  " + "Please enter all parameters");
    System.exit(1);
  }

  /**
   * Configures and runs a WordCount job over the Cloud Datastore connector.
   *
   * @param args a String[] containing your datasetId
   */
  public static void main(String[] args) throws Exception {
    GenericOptionsParser parser = new GenericOptionsParser(args);
    args = parser.getRemainingArgs();

    // Check all args entered.
    if (args.length != 4) {
      printUsageAndExit();
    }

    // Set parameters from args.
    String datasetId = args[0];
    String inputKindName = args[1];
    String outputKindName = args[2];
    String jobName = args[3];

    // Configure Map Reduce for WordCount job.
    JobConf conf = new JobConf(parser.getConfiguration(), WordCount.class);
    conf.set(OUTPUT_KIND_NAME, outputKindName);
    Job job = new Job(conf, jobName);
    job.setJarByClass(WordCount.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    // Set input and output classes.
    job.setInputFormatClass(DatastoreHadoopInputFormat.class);
    job.setOutputFormatClass(DatastoreHadoopOutputFormat.class);

    // Set WordCount query.
    Query.Builder q = Query.newBuilder();
    KindExpression.Builder kind = KindExpression.newBuilder();
    kind.setName(inputKindName);
    q.addKind(kind);
    q.setFilter(makeFilter(KEY_PROPERTY_NAME, PropertyFilter.Operator.HAS_ANCESTOR,
        makeValue(makeKey(inputKindName, WordCountSetUp.ANCESTOR_ENTITY_VALUE))));
    String query = TextFormat.printToString(q);

    // Set parameters for DatastoreHadoopInputFormat.
    DatastoreHadoopInputFormat.setInput(job, query, datasetId);

    // Set parameters for DatastoreHadoopInputFormat.
    String numEntitiesInBatch = "100";
    DatastoreHadoopOutputFormat.setOutputSpecs(job, datasetId, numEntitiesInBatch);

    job.waitForCompletion(true);
  }
}

