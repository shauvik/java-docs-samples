package com.google.cloud.hadoop.samples;

import static com.google.api.services.datastore.client.DatastoreHelper.makeKey;

import com.google.api.services.datastore.DatastoreV1.CommitRequest;
import com.google.api.services.datastore.DatastoreV1.Entity;
import com.google.api.services.datastore.DatastoreV1.EntityResult;
import com.google.api.services.datastore.DatastoreV1.Key;
import com.google.api.services.datastore.DatastoreV1.KindExpression;
import com.google.api.services.datastore.DatastoreV1.Mutation;
import com.google.api.services.datastore.DatastoreV1.Property;
import com.google.api.services.datastore.DatastoreV1.Query;
import com.google.api.services.datastore.DatastoreV1.QueryResultBatch;
import com.google.api.services.datastore.DatastoreV1.RunQueryRequest;
import com.google.api.services.datastore.DatastoreV1.RunQueryResponse;
import com.google.api.services.datastore.DatastoreV1.Value;
import com.google.api.services.datastore.client.Datastore;
import com.google.api.services.datastore.client.DatastoreException;
import com.google.cloud.hadoop.io.datastore.DatastoreHadoopHelper;
import com.google.protobuf.ByteString;

import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

/**
 * Sample program to set up dataset to run the Hadoop Wordcount example. Clears Entities with given
 * kinds and populates dataset with Entities of given kind.
 */
public class WordCountSetUp {
  // This is the value of the ancestor entity which will be shared by all the generated entities for
  // this WordCount setup. This allows the InputFormat of the WordCount job to later query entities
  // using this ancestor as part of the query filter.
  static final String ANCESTOR_ENTITY_VALUE = "ancestor";

  // Print sample usage and exit
  private static void printUsageAndExit() {
    System.out.println(
        "Usage: hadoop jar datastore_wordcountsetup.jar [datasetId] [inputKindName] "
            + "[outputKindName] [fileName].  " + "Please enter all parameters");
    System.exit(1);
  }

  /**
   * Clears and then populates the Datastore. Will delete all entities (of input and output kind) in
   * current Datastore
   *
   * @param args a String[] containing your datasetId
   */
  public static void main(String[] args) throws Exception {
    // Check all args entered.
    if (args.length != 4) {
      printUsageAndExit();
    }

    // Set parameters from args.
    String datasetId = args[0];
    String inputKindName = args[1];
    String outputKindName = args[2];
    String fileName = args[3];

    Datastore ds = null;
    try {
      // Setup the connection to Google Cloud Datastore and infer credentials
      // from the environment.
      DatastoreHadoopHelper helper = new DatastoreHadoopHelper(new Configuration());
      ds = helper.createDatastore(datasetId);

    } catch (IOException exception) {
      System.err.println("I/O error connecting to the datastore: " + exception.getMessage());
      System.exit(1);
    }
    // Delete INPUT_KIND_NAME and OUTPUT_KIND_NAME Entities.
    deleteAllOfKind(inputKindName, ds);
    deleteAllOfKind(outputKindName, ds);

    // Check INPUT_KIND_NAME and OUTPUT_KIND_NAME Entities deleted.
    checkDeleted(inputKindName, ds);
    checkDeleted(outputKindName, ds);

    // Populate datastore with INPUT_KIND_NAME Entities from file.
    populateDatastoreFromFile(inputKindName, fileName, ds);
  }

  /**
   * Deletes all Entites of Kind kind.
   *
   * @param kind the Kind of Entities to delete.
   * @param ds Datastore to use.
   */
  public static void deleteAllOfKind(String kind, Datastore ds) {
    // Create a query for all Entities of kind Line.
    Query.Builder q = Query.newBuilder();
    KindExpression.Builder kindBuilder = KindExpression.newBuilder();
    kindBuilder.setName(kind);
    q.addKind(kindBuilder);

    // Get all Entities of kind Line.
    RunQueryRequest request = RunQueryRequest.newBuilder().setQuery(q).build();
    RunQueryResponse response = null;
    try {
      response = ds.runQuery(request);
    } catch (DatastoreException e) {
      System.out.println("WARNING: can't erase root!" + e.getMessage());
    }

    // Iterate over all Entities of kind Line.
    List<EntityResult> result = null;
    while (response != null) {
      result = response.getBatch().getEntityResultList();

      // Delete each entity of kind Line
      CommitRequest.Builder bwRequest = CommitRequest.newBuilder();
      for (int i = 0; i < result.size(); i++) {
        Mutation.Builder mutation = Mutation.newBuilder();
        mutation.addDelete(result.get(i).getEntity().getKey());
        try {
          ds.commit(bwRequest.setMutation(mutation).setMode(CommitRequest.Mode.NON_TRANSACTIONAL)
              .build());
        } catch (DatastoreException e) {
          System.out.println(e.getMessage() + Arrays.toString(e.getStackTrace()));
        }
      }

      // If there are more results, get new results and delete these.
      if (response.getBatch().getMoreResults() == QueryResultBatch.MoreResultsType.NOT_FINISHED) {
        ByteString endCursor = response.getBatch().getEndCursor();
        q.setStartCursor(endCursor);
        try {
          response = ds.runQuery(request);
        } catch (DatastoreException e) {
          System.out.println("Datastore Error:" + e.getMessage());
        }
      } else {
        response = null;
      }
    }
  }

  /**
   * Checks all Entites of Kind kind are deleted.
   *
   * @param kind the Kind of Entities to check.
   * @param ds Datastore to use.
   */
  public static void checkDeleted(String kind, Datastore ds) throws InterruptedException {
    // Check that all records have been deleted.
    int count = 0;
    while (count > 1) {
      // Create a query for all Entities of kind Line.
      Query.Builder q = Query.newBuilder();
      KindExpression.Builder kindBuilder = KindExpression.newBuilder();
      kindBuilder.setName(kind);
      q.addKind(kindBuilder);
      RunQueryRequest request = RunQueryRequest.newBuilder().setQuery(q).build();
      RunQueryResponse response = null;
      try {
        response = ds.runQuery(request);
      } catch (DatastoreException e) {
        System.out.println("DatastoreError" + e.getMessage());
      }
      List<EntityResult> result = response.getBatch().getEntityResultList();
      count = result.size();
      System.out.println("Waiting for Datastore to update! Records:" + count);
      Thread.sleep(10000);
    }
  }

  /**
   * Populates datastore with Entities of Kind kind from file fileName.
   *
   * @param kind the Kind of Entities to delete.
   * @param fileName the name of the file to use.
   * @param ds Datastore to use.
   */
  public static void populateDatastoreFromFile(String kind, String fileName, Datastore ds)
      throws IOException, DatastoreException {
    // Get BufferedReader for file.
    FileInputStream fstream = new FileInputStream(fileName);
    DataInputStream in = new DataInputStream(fstream);
    BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));

    // Create ancestor key.
    Key ancestor = makeKey(kind, ANCESTOR_ENTITY_VALUE).build();

    // Set up Datastore request.
    CommitRequest.Builder request = CommitRequest.newBuilder();
    Mutation.Builder mutation = Mutation.newBuilder();
    try {
      // Iterate over each line and add Entity to Datastore.
      String line = "";
      int count = 0;
      while ((line = br.readLine()) != null) {
        Entity.Builder e = Entity.newBuilder().setKey(makeKey(ancestor, kind, count + 1));
        Value.Builder value = Value.newBuilder().setStringValue(line);
        e.addProperty(Property.newBuilder().setName("line").setValue(value.setIndexed(false)));
        mutation.addInsert(e);
        count++;
      }
    } finally {
      br.close();
    }
    // Commit request to Datastore.
    ds.commit(request.setMutation(mutation).setMode(CommitRequest.Mode.NON_TRANSACTIONAL).build());
  }

