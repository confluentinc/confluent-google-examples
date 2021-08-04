package com.ecuevas;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.Duration;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

public class DataflowPipeline {
    private static final Boolean VERBOSE = true;

    private static final Logger logger = LogManager.getLogger(DataflowPipeline.class);

    public static void main(String[] args) throws IOException {

        PipelineOptionsFactory.register(CustomPipelineOptions.class);
        CustomPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(CustomPipelineOptions.class);

        String username = decryptKey(options.getProject(), options.getRegion(), options.getkeyRing(), options.getkmsUsernameKeyId(), Base64.getDecoder().decode(options.getConfluentCloudEncryptedUsername().getBytes("UTF-8")));
        String password = decryptKey(options.getProject(), options.getRegion(), options.getkeyRing(), options.getkmsPasswordKeyId(), Base64.getDecoder().decode(options.getConfluentCloudEncryptedPassword().getBytes("UTF-8")));

        Map<String, Object> props = new HashMap<>();
        props.put("auto.offset.reset", "earliest");
        props.put("ssl.endpoint.identification.algorithm", "https");
        props.put("sasl.mechanism", "PLAIN");
        props.put("request.timeout.ms", 20000);
        props.put("retry.backoff.ms", 500);
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config",String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",username, password));

        LogKafkaMsg logKafkaMsg = new LogKafkaMsg();

        Pipeline pipeline = Pipeline.create(options);

        PCollection<KV<String, String>> entries =
                pipeline
                        .apply(
                                "Read Entries from Confluent Cloud Topic",
                                KafkaIO.<String, String>read()
                                        .withBootstrapServers("<your-bootstrap-server>")
                                        .withTopic("entries")
                                        .withConsumerConfigUpdates(props)
                                        .withKeyDeserializer(StringDeserializer.class)
                                        .withValueDeserializer(StringDeserializer.class)
                                        .withoutMetadata()
                        );

        PCollection<KV<String,String>> triggeredGlobalWindowEntries = entries
                .apply(
                "(Kafka) Trigger",
                Window.<KV<String,String>>into(new GlobalWindows())
                        // Get periodic results every minute.
                        .triggering(
                                Repeatedly.forever(
                                        AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(60))))
                        .discardingFiredPanes());
        if(VERBOSE) {
            entries.apply(
                    "Log Kafka Messages",
                    ParDo.of(logKafkaMsg));
        }

        PCollection<TableRow> rows =
                pipeline
                        .apply(
                                "Read Promotions from BigQuery",
                                BigQueryIO.readTableRows()
                                        .fromQuery(String.format("SELECT * FROM `%s.%s.%s` WHERE day = CURRENT_DATE()", options.getProject(), "promotions", "dailygiftcardwinners"))
                                        .usingStandardSql()
                        );

        PCollection<KV<String, TableRow>> rowsKeyedByUser = rows
                .apply(
                        "Key Rows",
                        WithKeys.of(new SerializableFunction<TableRow, String>() {
                            @Override
                            public String apply(TableRow row) {
                                String day = (String)row.get("day");
                                String winnerOffset = (String) row.get("winnernumber");
                                String rowKey = day.replace("-","")+"_"+winnerOffset;
                                return rowKey;
                            }
                }))
                .apply(
                        "(BQ) Trigger",
                        Window.<KV<String,TableRow>>into(new GlobalWindows())
                                // Get periodic results every minute.
                                .triggering(
                                        Repeatedly.forever(
                                                AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(60))))
                                .accumulatingFiredPanes());

        if(VERBOSE) {
            rowsKeyedByUser.apply(
                    "Log BQ Results",
                    ParDo.of(new logBQRows()));
        }

        final TupleTag<String> entriesTag = new TupleTag<String>();
        final TupleTag<TableRow> promosTag = new TupleTag<TableRow>();

        // Merge collection values into a CoGbkResult collection.
        PCollection<KV<String, CoGbkResult>> joinedCollection =
                KeyedPCollectionTuple
                        .of(entriesTag, triggeredGlobalWindowEntries)
                        .and(promosTag, rowsKeyedByUser)
                        .apply(
                                "Join Promotions and Entries",
                                CoGroupByKey.<String>create()
                        );

        if(VERBOSE) {
            joinedCollection.apply("Log Winners",
                    ParDo.of(new logWinners(entriesTag, promosTag)));
        }

        PCollection<TableRow> joinedBQRows = joinedCollection
                .apply("Transform to TableRow",
                        ParDo.of(new CoGbkResultToTableRow(entriesTag,promosTag))
                        );

        TableSchema schema = new TableSchema()
                .setFields(
                        Arrays.asList(
                                new TableFieldSchema()
                                        .setName("entry_time")
                                        .setType("FLOAT64")
                                        .setMode("NULLABLE"),
                                new TableFieldSchema()
                                        .setName("winner")
                                        .setType("BOOL")
                                        .setMode("NULLABLE"),
                                new TableFieldSchema()
                                        .setName("name")
                                        .setType("STRING")
                                        .setMode("NULLABLE"),
                                new TableFieldSchema()
                                        .setName("participationnumber")
                                        .setType("FLOAT64")
                                        .setMode("NULLABLE"),
                                new TableFieldSchema()
                                        .setName("day")
                                        .setType("STRING")
                                        .setMode("NULLABLE"),
                                new TableFieldSchema()
                                        .setName("email")
                                        .setType("STRING")
                                        .setMode("NULLABLE")

                        )
                );



        joinedBQRows.apply(
                "Write Entries BigQuery",
                BigQueryIO.writeTableRows()
                        .to(String.format("%s:%s.%s", options.getProject(), "raffle_dataset", "dailyentries"))
                        .withSchema(schema)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_APPEND));

        PCollection <KV<String,String>> joinedKVCollection = joinedCollection
                .apply("Transform to KV",
                        ParDo.of(new CoGbkResultToKV(entriesTag,promosTag)));

        joinedKVCollection.apply(
                "Write to Confluent",
                KafkaIO.<String, String>write()
                        .withBootstrapServers("<your-bootstrap-server>")
                        .withTopic("application")
                        .withProducerConfigUpdates(props)
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer(StringSerializer.class)
        );

        pipeline.run().waitUntilFinish();
    }



    public static final class CoGbkResultToTableRow extends DoFn<KV<String, CoGbkResult>, TableRow> {
        private final TupleTag<String> entriesTag;
        private final TupleTag<TableRow> promosTag;

        CoGbkResultToTableRow(final TupleTag<String> entriesTag, final TupleTag<TableRow> promosTag) {
            this.entriesTag = entriesTag;
            this.promosTag = promosTag;
        }

        /**
         * ProcessElement method for BEAM.
         *
         * @param c Process context.
         * @throws Exception Exception on the way.
         */
        @ProcessElement
        public void processElement(final ProcessContext c) throws Exception {
            final KV<String, CoGbkResult> kv = c.element();
            final Boolean isWinner = !kv.getValue().getOnly(promosTag, new TableRow()).isEmpty();

            final String entryJSON = kv.getValue().getOnly(entriesTag,"");

            Gson gson = new Gson();

            if (entryJSON != "") {
                JSONObject mainObject = new JSONObject(entryJSON);
                mainObject.put("winner", isWinner);
                final TableRow outputRow = gson.fromJson(mainObject.toString(), TableRow.class);
                if(VERBOSE) {
                    logger.info("### App Logging | TableRow | outputRow: " + outputRow);
                }
                c.output(outputRow);
            }
        }

    }

    public static final class CoGbkResultToKV extends DoFn<KV<String, CoGbkResult>, KV<String,String>> {
        private final TupleTag<String> entriesTag;
        private final TupleTag<TableRow> promosTag;

        CoGbkResultToKV(final TupleTag<String> entriesTag, final TupleTag<TableRow> promosTag) {
            this.entriesTag = entriesTag;
            this.promosTag = promosTag;
        }

        /**
         * ProcessElement method for BEAM.
         *
         * @param c Process context.
         * @throws Exception Exception on the way.
         */
        @ProcessElement
        public void processElement(final ProcessContext c) throws Exception {
            final KV<String, CoGbkResult> kv = c.element();
            final Boolean isWinner = !kv.getValue().getOnly(promosTag, new TableRow()).isEmpty();

            final String entryJSON = kv.getValue().getOnly(entriesTag,"");
            JSONObject mainObject = new JSONObject(entryJSON);

            mainObject.put("winner", isWinner);

            Gson gson = new Gson();

            if (entryJSON != "") {
                final KV<String,String> outputKV = KV.of(kv.getKey(),mainObject.toString());

                if(VERBOSE) {
                    logger.info("### App Logging | KV<S,S> | outputKV: " + outputKV);
                }
                c.output(outputKV);
            }
        }

    }

    public static final class logWinners extends DoFn<KV<String, CoGbkResult>, KV<String, String>> {
        private final TupleTag<String> entriesTag;
        private final TupleTag<TableRow> promosTag;

        logWinners(final TupleTag<String> entriesTag, final TupleTag<TableRow> promosTag) {
            this.entriesTag = entriesTag;
            this.promosTag = promosTag;
        }

        /**
         * ProcessElement method for BEAM.
         *
         * @param c Process context.
         * @throws Exception Exception on the way.
         */
        @ProcessElement
        public void processElement(final ProcessContext c) throws Exception {
            final KV<String, CoGbkResult> kv = c.element();
            final String entryKey = kv.getKey();

            logger.info("### App Logging | Winner | key: "+entryKey);
            logger.info("### App Logging | Winner Details | isWinner: "+!kv.getValue().getOnly(promosTag, new TableRow()).isEmpty()+" value: "+kv.getValue());
        }

    }

    public static class LogKafkaMsg extends DoFn<KV<String, String>, KV<String, String>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            logger.info("### App Logging | Entry | key: " + c.element().getKey() + " value: " + c.element().getValue());
        }
    }

    public static class logBQRows extends DoFn<KV<String, TableRow>, KV<String, TableRow>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            logger.info("### App Logging | BQ Row | key: " + c.element().getKey() + " value: " + c.element().getValue());
        }
    }

    public static String decryptKey(String gcpProject, String location, String keyRing, String kmsKey, byte[] ciphertext) throws IOException {
        try (KeyManagementServiceClient client = KeyManagementServiceClient.create()) {

            CryptoKeyName keyName = CryptoKeyName.of(gcpProject, location, keyRing, kmsKey);
            DecryptResponse response = client.decrypt(keyName, ByteString.copyFrom(ciphertext));

            return response.getPlaintext().toStringUtf8();
        }
    }
}