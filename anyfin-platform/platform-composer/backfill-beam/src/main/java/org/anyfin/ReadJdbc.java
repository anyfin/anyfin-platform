package org.anyfin;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;

import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.ZoneOffset;
import java.time.Instant;
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
import org.apache.beam.sdk.transforms.Create;
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

        @Description("Query Breakdown - daily/full")
        @Default.String("daily")
        StaticValueProvider<String> getQueryBreakdown();
        void setQueryBreakdown(StaticValueProvider<String> value);

        @Description("Credentials - JSON file with DB credentials")
        @Default.String("main.json")
        StaticValueProvider<String> getCredentialsFile();
        void setCredentialsFile(StaticValueProvider<String> value);
    }
    
    private static final String minDate = "2017-10-26";
    private static final String driver = "org.postgresql.Driver";

    static class ToKVFn extends DoFn<String, KV<String, Integer>> {
        @ProcessElement
        public void processElement(@Element String word, OutputReceiver<KV<String, Integer>> out) {
          out.output(KV.of(word, 1));
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

        String currentDate = Instant.now().atZone(ZoneOffset.UTC).format(DateTimeFormatter.ISO_LOCAL_DATE);

        DateRange dateRange = new DateRange(LocalDate.parse(minDate), LocalDate.parse(currentDate));

        List<String> datesDaily = dateRange.toStringList();
        List<String> datesEarliest = dateRange.toStringListEarliest();

        String selectStatement = "select *, now() as _ingested_ts from ";

        String queryTemplateFull = " where created_at >= to_date(?::text, 'YYYYMMDD')::date";

        String queryTemplateDaily = " where created_at >= to_date(?::text, 'YYYYMMDD')::date "
        + "and created_at < (to_date(?::text, 'YYYYMMDD')::date + 1)";

        DBConfig dbConfig = new DBConfig();
        try {
            ObjectMapper mapper = new ObjectMapper();
            dbConfig = mapper.readValue(Paths.get(options.getCredentialsFile().get()).toFile(), DBConfig.class);
            System.out.println(dbConfig);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        String queryTemplate = queryTemplateDaily;
        List<String> dates = datesDaily;
        
        if (!options.getQueryBreakdown().toString().equals("daily")) {
            queryTemplate = queryTemplateFull;
            dates = datesEarliest;
        }

        Pipeline pipeline = Pipeline.create(options);

        pipeline
            .apply("Generate queries", Create.of(dates)).setCoder(StringUtf8Coder.of())
            .apply("Split to KV", ParDo.of(new ToKVFn()))
            .apply("Reshuffle", GroupByKey.create())
            .apply("Read from DB", JdbcIO.<KV<String,Iterable<Integer>>, TableRow>readAll()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(driver, dbConfig.getLocation())
                .withUsername(dbConfig.getUsername())
                .withPassword(dbConfig.getPassword()))
                .withFetchSize(1000)
                .withCoder(TableRowJsonCoder.of())
                .withParameterSetter(new JdbcIO.PreparedStatementSetter<KV<String,Iterable<Integer>>>() {
                  @Override
                  public void setParameters(KV<String,Iterable<Integer>> element, PreparedStatement preparedStatement) throws Exception {
                    preparedStatement.setInt(1, Integer.parseInt(element.getKey()));
                    preparedStatement.setInt(2, Integer.parseInt(element.getKey()));
                  }
                })
                .withOutputParallelization(false)
                .withQuery(selectStatement + options.getSourceTable().get() + queryTemplate)
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