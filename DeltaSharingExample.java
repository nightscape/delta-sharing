///usr/bin/env jbang
//DEPS io.delta:delta-sharing-client_2.12:1.3.0
//DEPS dev.mauch:knox-webhdfs:0.0.6
//DEPS org.apache.spark:spark-sql_2.12:3.3.4
//DEPS org.scala-lang:scala-library:2.12.20
//DEPS org.apache.hadoop:hadoop-client:3.3.5
//DEPS org.apache.parquet:parquet-column:1.12.2
//DEPS org.apache.parquet:parquet-hadoop-bundle:1.12.2
//RUNTIME_OPTIONS -Djavax.net.ssl.trustStore=./truststore/truststore.p12
//RUNTIME_OPTIONS -Djavax.net.ssl.trustStorePassword=thekeystorespasswd

import io.delta.sharing.client.*;
import io.delta.sharing.client.model.*;
import scala.Option;
import scala.Option$;
import scala.Some;
import scala.collection.JavaConverters$;
import scala.collection.Seq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.util.List;
import java.util.ServiceLoader;
import java.util.UUID;

public class DeltaSharingExample {
    public static void main(String[] args) {
        // Add ServiceLoader for FileSystem
        ServiceLoader<FileSystem> loader = ServiceLoader.load(FileSystem.class);
        System.out.println("Discovered FileSystem implementations:");
        for (FileSystem fs : loader) {
            System.out.println(fs.getClass().getName());
        }
        System.out.println("------------------------------------");

        DeltaSharingProfileProvider profileProvider = new SimpleDeltaSharingProfileProvider();
        DeltaSharingRestClient client = new DeltaSharingRestClient(
            profileProvider,
            120,                            // timeoutInSeconds
            10,                             // numRetries
            Long.MAX_VALUE,                 // maxRetryDuration
            1000,                           // retrySleepInterval
            true,                           // sslTrustAll
            false,                          // forStreaming
            "parquet",                    // responseFormat
            "",                           // readerFeatures
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
            System.out.println("Listing all tables:");
            List<Table> tables = toList(client.listAllTables());
            for (Table table : tables) {
                System.out.println("Table: " + table.share() + "." + table.schema() + "." + table.name());
            }
            tables.stream().filter(t -> t.name().equals("test-table")).forEach(table -> {
                System.out.println("Table: " + table.share() + "." + table.schema() + "." + table.name());

                Table myTable = table;
                DeltaTableMetadata metadata = client.getMetadata(myTable, None(), None());
                System.out.println("\nMetadata Version: " + metadata.version());
                List<String> lines = toList(metadata.lines());
                System.out.println("Metadata Lines:");
                for (String line : lines) {
                    System.out.println(line);
                }

                System.out.println("\nRetrieving Parquet data for table:");
                Seq<String> predicates = scala.collection.JavaConverters.asScalaBufferConverter(
                    java.util.Arrays.asList(
                      "id = 1"
                    )
                ).asScala().toSeq();
                DeltaTableFiles tableFiles = client.getFiles(myTable, predicates, None(), None(), None(), None(), None());
                System.out.println("DeltaTableFiles Version: " + tableFiles.version());
                List<AddFile> filesList = toList(tableFiles.files());
                for (AddFile addFile : filesList) {
                    System.out.println("Parquet file URL: " + addFile.url());
                }

                if (!filesList.isEmpty()) {
                    // Setup Hadoop configuration
                    Configuration conf = new Configuration();

                    for (AddFile addFile : filesList) {
                        String fileUrl = addFile.url();
                        System.out.println("Reading Parquet data from: " + fileUrl);

                        try {
                            // Process the file based on its URL type
                            processParquetFile(fileUrl, conf);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            client.close();
        }
    }

    /**
     * Process a Parquet file based on its URL type.
     *
     * @param fileUrl The URL of the Parquet file
     * @param conf The Hadoop configuration
     * @throws Exception If an error occurs during processing
     */
    private static void processParquetFile(String fileUrl, Configuration conf) throws Exception {
        // Determine if this is a direct HDFS/WebHDFS URL or an HTTP/HTTPS URL
        boolean isDirectHdfsUrl = isDirectHdfsUrl(fileUrl);

        if (isDirectHdfsUrl) {
            // For HDFS/WebHDFS URLs, try to read directly using Hadoop APIs
            System.out.println("Using direct Hadoop API access for: " + fileUrl);
            readParquetFileDirectly(fileUrl, conf);
        } else {
            // For HTTP/HTTPS URLs, download to a temporary file first
            System.out.println("Using HTTP download for: " + fileUrl);
            readParquetFileViaDownload(fileUrl, conf);
        }
    }

    /**
     * Determines if a URL is a direct HDFS/WebHDFS URL that can be accessed via Hadoop APIs.
     *
     * @param urlString The URL to check
     * @return true if it's a direct HDFS/WebHDFS URL, false otherwise
     */
    private static boolean isDirectHdfsUrl(String urlString) {
        try {
            URI uri = new URI(urlString);
            String scheme = uri.getScheme();
            if (scheme == null) return false;

            // Check if it's a direct HDFS URL
            return scheme.equalsIgnoreCase("hdfs") ||
                   scheme.equalsIgnoreCase("file") ||
                   (scheme.equalsIgnoreCase("webhdfs") && !urlString.contains("?")) ||
                   (scheme.equalsIgnoreCase("swebhdfs") && !urlString.contains("?")) ||
                   scheme.equalsIgnoreCase("knoxwebhdfs") ||
                   scheme.equalsIgnoreCase("knoxswebhdfs");
        } catch (URISyntaxException e) {
            return false;
        }
    }

    /**
     * Reads a Parquet file directly using Hadoop APIs.
     *
     * @param fileUrl The URL of the Parquet file
     * @param conf The Hadoop configuration
     * @throws Exception If an error occurs during reading
     */
    private static void readParquetFileDirectly(String fileUrl, Configuration conf) throws Exception {
        try {
            // Create a Hadoop Path from the URL
            org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(fileUrl);

            // Read Parquet file using ParquetReader
            try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
                    .withConf(conf)
                    .build()) {
                Group record;
                while ((record = reader.read()) != null) {
                    System.out.println(record.toString());
                }
            }
        } catch (Exception e) {
            System.err.println("Failed to read directly, falling back to download: " + e.getMessage());
            // Fall back to download if direct reading fails
            readParquetFileViaDownload(fileUrl, conf);
        }
    }

    /**
     * Reads a Parquet file by first downloading it to a temporary file.
     *
     * @param fileUrl The URL of the Parquet file
     * @param conf The Hadoop configuration
     * @throws Exception If an error occurs during reading
     */
    private static void readParquetFileViaDownload(String fileUrl, Configuration conf) throws Exception {
        // Create a temporary file to download the Parquet data
        java.nio.file.Path tempFilePath = Files.createTempFile("delta-sharing-", ".parquet");
        File tempFile = tempFilePath.toFile();
        tempFile.deleteOnExit(); // Clean up when the JVM exits

        System.out.println("Downloading to temporary file: " + tempFilePath);

        // Download the file using direct HTTP connection
        downloadFile(fileUrl, tempFilePath.toString());

        System.out.println("Download complete. Reading Parquet file...");

        // Create a Hadoop Path from the local file
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(tempFilePath.toString());

        // Read Parquet file using ParquetReader
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
                .withConf(conf)
                .build()) {
            Group record;
            while ((record = reader.read()) != null) {
                System.out.println(record.toString());
            }
        } finally {
            // Clean up the temporary file
            tempFile.delete();
        }
    }

    /**
     * Downloads a file from a URL to a local file path.
     *
     * @param urlString The URL to download from
     * @param filePath The local file path to save to
     * @throws Exception If an error occurs during download
     */
    private static void downloadFile(String urlString, String filePath) throws Exception {
        URL url = new URL(urlString);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");

        // Set up Basic Authentication if username and password are provided
        String username = System.getProperty("delta.sharing.username");
        String password = System.getProperty("delta.sharing.password");
        if (username != null && password != null) {
            String auth = username + ":" + password;
            String encodedAuth = java.util.Base64.getEncoder().encodeToString(auth.getBytes("UTF-8"));
            connection.setRequestProperty("Authorization", "Basic " + encodedAuth);
            System.err.println("Added Basic Authentication for user: " + username);
        }

        // Set up SSL trust all if needed
        if (urlString.startsWith("https")) {
            javax.net.ssl.HttpsURLConnection httpsConn = (javax.net.ssl.HttpsURLConnection) connection;
            httpsConn.setHostnameVerifier((hostname, session) -> true);
        }

        // Connect and check response code
        connection.connect();
        int responseCode = connection.getResponseCode();

        if (responseCode != HttpURLConnection.HTTP_OK) {
            throw new Exception("HTTP error: " + responseCode + " - " + connection.getResponseMessage());
        }

        // Download the file
        try (InputStream inputStream = connection.getInputStream();
             FileOutputStream outputStream = new FileOutputStream(filePath)) {

            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
        }

        System.out.println("File downloaded successfully to: " + filePath);
    }

    public static class SimpleDeltaSharingProfileProvider implements DeltaSharingProfileProvider {
        @Override
        public DeltaSharingProfile getProfile() {
            // Change the URL below to your Delta Sharing server endpoint.
            return DeltaSharingProfile$.MODULE$.apply(new Some<>(1), "http://localhost:8001/delta-sharing", "dapi5e3574ec767ca1548ae5bbed1a2dc04d", "theexpirationtime");
        }
    }

    public static <T> List<T> toList(Seq<T> seq) {
        return JavaConverters$.MODULE$.seqAsJavaList(seq);
    }
    public static <T> Option<T> None() {
        return Option$.MODULE$.empty();
    }
}
