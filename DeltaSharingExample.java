///usr/bin/env jbang
//DEPS io.delta:delta-sharing-client_2.12:1.2.2
//DEPS org.apache.spark:spark-sql_2.12:3.3.4
//DEPS org.scala-lang:scala-library:2.12.20

import io.delta.sharing.client.*;
import io.delta.sharing.client.model.*;
import scala.Option;
import scala.Option$;
import scala.Some;
import scala.collection.Seq;
import scala.collection.JavaConverters$;
import org.apache.spark.sql.*;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

public class DeltaSharingExample {

    public static void main(String[] args) {
        // Create a simple profile provider that returns a minimal profile with an endpoint.
        DeltaSharingProfileProvider profileProvider = new SimpleDeltaSharingProfileProvider();

        // Instantiate the DeltaSharingRestClient with required parameters.
        // Note: The parameters below mirror those used in tests.
        DeltaSharingRestClient client = new DeltaSharingRestClient(
                profileProvider,
                120,                            // timeoutInSeconds
                10,                             // numRetries
                Long.MAX_VALUE,                 // maxRetryDuration
                true,                           // sslTrustAll
                false,                          // forStreaming
                "parquet",                      // responseFormat
                "",                             // readerFeatures
                false,                          // queryTablePaginationEnabled
                100000,                         // maxFilesPerReq
                false,                          // endStreamActionEnabled
                false,                          // enableAsyncQuery
                10000L,                         // asyncQueryPollIntervalMillis
                600000L,                        // asyncQueryMaxDuration
                5,                              // tokenExchangeMaxRetries
                60,                             // tokenExchangeMaxRetryDurationInSeconds
                600                             // tokenRenewalThresholdInSeconds
        );

        try {
            // Example 1: List all tables available from the sharing server.
            System.out.println("Listing all tables:");
            for (Table table : toList(client.listAllTables())) {
                // Since Table is a Scala case class, access its fields via the generated methods.
                System.out.println("Table: " + table.share() + "." + table.schema() + "." + table.name());
            }

            // Example 2: Get metadata for a specific table.
            // Modify the table details (share, schema, table name) accordingly.
            Table myTable = new Table("test-table", "test-schema", "test-share");
            DeltaTableMetadata metadata = client.getMetadata(myTable, None(), None());
            System.out.println("\nMetadata Version: " + metadata.version());

            // The metadata.lines field is a Scala Seq<String>. Convert it to a Java List for easier iteration.
            List<String> lines = toList(metadata.lines());
            System.out.println("Metadata Lines:");
            for (String line : lines) {
                System.out.println(line);
            }

            // --- New code to retrieve Parquet data from the table ---
            System.out.println("\nRetrieving Parquet data for table:");
            // Create an empty Scala Seq for predicates
            scala.collection.Seq<String> emptyPredicates = scala.collection.immutable.List$.MODULE$.empty();
            DeltaTableFiles tableFiles = client.getFiles(myTable, emptyPredicates, None(), None(), None(), None(), None());
            System.out.println("DeltaTableFiles Version: " + tableFiles.version());
            List<AddFile> filesList = toList(tableFiles.files());
            for (AddFile addFile : filesList) {
                System.out.println("Parquet file URL: " + addFile.url());
            }

            if (!filesList.isEmpty()) {
                AddFile firstFile = filesList.get(0);
                String fileUrl = firstFile.url();
                System.out.println("Reading Parquet data from: " + fileUrl);
                SparkSession spark = SparkSession.builder()
                        .appName("DeltaSharingParquetReadExample")
                        .master("local[*]")
                        .getOrCreate();
                Configuration hadoopConf = new Configuration();
                hadoopConf.set("fs.defaultFS", "hdfs://localhost:8020");
                // Set Kerberos as the authentication method.
                hadoopConf.set("hadoop.security.authentication", "kerberos");
                // Optionally, you can set additional Kerberos-related properties if required
                hadoopConf.set("dfs.namenode.kerberos.principal", "nn/9280d4379405@HADOOP.LOCAL");
                // Initialize the security configuration and login using your keytab.
                UserGroupInformation.setConfiguration(hadoopConf);
                UserGroupInformation.loginUserFromKeytab("tester@HADOOP.LOCAL", "/tmp/tester.keytab");

                Dataset<Row> df = spark.read().parquet(fileUrl);
                df.show();
                spark.stop();
            }
            // --- End new code
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.close();
        }
    }

    public static class SimpleDeltaSharingProfileProvider implements DeltaSharingProfileProvider {
            @Override
            public DeltaSharingProfile getProfile() {
                // Change the URL below to your Delta Sharing server endpoint.
                return DeltaSharingProfile$.MODULE$.apply(new Some<>(1), "http://localhost:8000/delta-sharing", "dapi5e3574ec767ca1548ae5bbed1a2dc04d", "theexpirationtime");
            }
        }

    public static <T> List<T> toList(Seq<T> seq) {
        return JavaConverters$.MODULE$.seqAsJavaList(seq);
    }
    public static <T> Option<T> None() {
        return Option$.MODULE$.empty();
    }
}
