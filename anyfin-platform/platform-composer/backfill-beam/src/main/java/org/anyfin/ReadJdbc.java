package org.anyfin;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;

import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;


public class ReadJdbc {

    private static final Logger LOG = LoggerFactory.getLogger(ReadJdbc.class);

    public interface BackfillerOptions extends PipelineOptions {
        @Description("Source Table")
        @Default.String("source_table")
        ValueProvider<String> getSourceTable();
        void setSourceTable(ValueProvider<String> value);

        @Description("Destination Table - <project>:<dataset>.<table>")
        @Default.String("project:dataset.table")
        ValueProvider<String> getDestinationTable();
        void setDestinationTable(ValueProvider<String> value);

        @Description("Step Size - How many rows per query should be read")
        @Default.String("50000")
        StaticValueProvider<String> getStepSize();
        void setStepSize(StaticValueProvider<String> value);

        @Description("Credentials - JSON file with DB credentials")
        @Default.String("main.json")
        StaticValueProvider<String> getCredentialsFile();
        void setCredentialsFile(StaticValueProvider<String> value);
    }
    
    private static final String driver = "org.postgresql.Driver";

    static class PrintFn extends DoFn<KV<String, Iterable<Integer>>, KV<String, Iterable<Integer>>> {
        @ProcessElement
        public void processElement(@Element KV<String, Iterable<Integer>> query, OutputReceiver<KV<String, Iterable<Integer>>> out) {
          System.out.println(query);
          out.output(query);
        }
    }

    static class ToKVFn extends DoFn<String, KV<String, Integer>> {
        @ProcessElement
        public void processElement(@Element String word, OutputReceiver<KV<String, Integer>> out) {
          out.output(KV.of(word, 1));
        }
    }

    static class GenerateQueriesFn extends DoFn<String, String> {
        ValueProvider<String> stepSize;

        GenerateQueriesFn(StaticValueProvider<String> stepSize) {
            this.stepSize = stepSize;
        }

        @ProcessElement
        public void processElement(ProcessContext cnt) {
            int totalRows = Integer.parseInt(cnt.element());
            int intStepSize = Integer.parseInt(stepSize.get());
            //Adding two more offsets to make sure we don't miss newly created rows
            for(int i = 0; i<(totalRows + 2*intStepSize); i+=intStepSize) {
                cnt.output(String.format("%s", i));
            }
        }
    }

    static class ConvertFn extends DoFn<TableRow, TableRow> {
        @ProcessElement
        public void processElement(@Element TableRow row, OutputReceiver<TableRow> out) {
          TableRow outputRow = new TableRow();
          // array_fields are fields that are of type text[] in postgres and will be REPEATED in BQ
          String[] array_fields = {"file_urls", "reject_tags"};
          outputRow = row.clone();

          for (String field : array_fields) {
              Object field_obj = outputRow.get(field);
              if (field_obj instanceof String) {
                  List<String> listarr = Arrays.asList(field_obj.toString().replaceAll("[{}]", " ").split(","));
                  outputRow.set(field, listarr);
              }
          }
          out.output(outputRow);
        }
    }

    private static class DBConfig {
        private String location;
        private String username;
        private String password;
        
        public String getLocation() {
            return location;
        }

        public String getPassword() {
            return password;
        }

        public String getUsername() {
            return username;
        }
    }

    static void runReadJdbc(BackfillerOptions options) {

        DBConfig dbConfig = new DBConfig();
        try {
            ObjectMapper mapper = new ObjectMapper();
            dbConfig = mapper.readValue(Paths.get(options.getCredentialsFile().get()).toFile(), DBConfig.class);
            System.out.println(dbConfig);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        Pipeline pipeline = Pipeline.create(options);

        pipeline
            .apply("Get Rowcount", JdbcIO.<String>read()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(driver, dbConfig.getLocation())
                .withUsername(dbConfig.getUsername())
                .withPassword(dbConfig.getPassword()))
                .withQuery("select count(*)::text from " + options.getSourceTable().get())
                .withRowMapper((JdbcIO.RowMapper<String>) resultSet -> {
                    return resultSet.getString(1);
                })
                .withOutputParallelization(false)
                .withCoder(StringUtf8Coder.of()))
            .apply("Generate Queries", ParDo.of(new GenerateQueriesFn(options.getStepSize())))
            .apply("Split to KV", ParDo.of(new ToKVFn()))
            .apply("Reshuffle", GroupByKey.create())
            // .apply(ParDo.of(new PrintFn()))
            .apply("Read from DB", JdbcIO.<KV<String,Iterable<Integer>>, TableRow>readAll()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(driver, dbConfig.getLocation())
                .withUsername(dbConfig.getUsername())
                .withPassword(dbConfig.getPassword()))
                // .withFetchSize(1000)
                .withCoder(TableRowJsonCoder.of())
                .withParameterSetter(new JdbcIO.PreparedStatementSetter<KV<String,Iterable<Integer>>>() {
                  @Override
                  public void setParameters(KV<String,Iterable<Integer>> element, PreparedStatement preparedStatement) throws Exception {
                    preparedStatement.setInt(1, Integer.parseInt(element.getKey()));
                  }
                })
                .withOutputParallelization(false)
                .withQuery(String.format("%s %s limit %s %s",
                    "select *, now() as _ingested_ts from ", 
                    options.getSourceTable().get(),
                    options.getStepSize().get(), 
                    " offset ?"))
                .withRowMapper((JdbcIO.RowMapper<TableRow>) resultSet -> {
                    TableRow row = new TableRow();
                    
                    for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                        String columnTypeIntKey ="";
                        try {
                            row.set(resultSet.getMetaData().getColumnName(i).toString(), resultSet.getString(i));
                        } catch (Exception e) {
                            LOG.error("problem columnTypeIntKey: " +  columnTypeIntKey);
                            throw e;
                        }
                    }
                    return row;
                })
            )
            .apply("Convert", ParDo.of(new ConvertFn()))
            .apply("Write to BigQuery", BigQueryIO.writeTableRows()
                .withoutValidation()
                .to(options.getDestinationTable())
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
            );

        PipelineResult result = pipeline.run();
        try {
            result.getState();
            result.waitUntilFinish();
        } catch (UnsupportedOperationException e) {
            // do nothing - this is to avoid getting this exception after template is built
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        BackfillerOptions options = PipelineOptionsFactory.fromArgs(args).as(BackfillerOptions.class);

        runReadJdbc(options);
    }
}