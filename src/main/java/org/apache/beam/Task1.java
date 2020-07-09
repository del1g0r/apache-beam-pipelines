package org.apache.beam;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class Task1 {

    public interface Task1Options extends PipelineOptions {

        @Description("Path of the file to read Users from")
        @Default.String("src/main/resources/users.avro")
        String getUserFile();

        void setUserFile(String value);

        @Description("Path of the file to read Ages from")
        @Default.String("src/main/resources/ageString.avro")
        String getAgeFile();

        void setAgeFile(String value);

        @Description("Path of the file to write to")
        @Required
        String getOutput();

        void setOutput(String value);

        @Description("Path of the error file to write to")
        @Required
        String getErrorOutput();

        void setErrorOutput(String value);
    }

    static final class Field {
        static final Schema.Field EMAIL = Schema.Field.of("email", Schema.FieldType.STRING);
        static final Schema.Field AGE = Schema.Field.of("age", Schema.FieldType.INT64);
        static final Schema.Field USER_NAME = Schema.Field.of("userName", Schema.FieldType.STRING);
        static final Schema.Field SHORT_USER_NAME = Schema.Field.of("shortUserName", Schema.FieldType.STRING);
        static final Schema.Field AGE_STRING = Schema.Field.of("ageString", Schema.FieldType.STRING);
        static final Schema.Field ERROR = Schema.Field.of("error", Schema.FieldType.STRING);
    }

    static final Schema USER_SCHEMA = Schema.of(Field.EMAIL, Field.AGE, Field.USER_NAME);
    static final Schema AGE_STRING_SCHEMA = Schema.of(Field.AGE, Field.AGE_STRING);
    static final Schema RESULT_SCHEMA = Schema.of(Field.EMAIL, Field.AGE, Field.USER_NAME, Field.AGE_STRING, Field.SHORT_USER_NAME);
    static final Schema ERROR_SCHEMA = Schema.of(Field.EMAIL, Field.AGE, Field.USER_NAME, Field.ERROR);

    static final String UNKNOWN = "unknown";

    static void runTask1(Task1Options options) {

        final TupleTag<GenericRecord> resultTag = new TupleTag<>();
        final TupleTag<GenericRecord> errorTag = new TupleTag<>();

        Pipeline p = Pipeline.create(options);

        PCollection<GenericRecord> users = p
                .apply("ReadUsers", AvroIO.readGenericRecords(AvroUtils.toAvroSchema(USER_SCHEMA)).from(options.getUserFile()));

        PCollectionView<Map<Long, GenericRecord>> ageStrings = p
                .apply("ReadAges", AvroIO.readGenericRecords(AvroUtils.toAvroSchema(AGE_STRING_SCHEMA)).from(options.getAgeFile()))
                .apply(WithKeys.of(new SerializableFunction<GenericRecord, Long>() {
                    @Override
                    public Long apply(GenericRecord s) {
                        return (Long) s.get(Field.AGE.getName());
                    }
                }))
                .apply(View.asMap());

        PCollectionTuple result = users.apply(
                "Process",
                ParDo.of(new DoFn<GenericRecord, GenericRecord>() {

                             @ProcessElement
                             public void processElement(ProcessContext c) {
                                 GenericRecord element = c.element();
                                 String email = element.get(Field.EMAIL.getName()).toString();
                                 String userName = element.get(Field.USER_NAME.getName()).toString();
                                 Long age = (Long) element.get(Field.AGE.getName());
                                 GenericRecord sideInputData = c.sideInput(ageStrings).get(age);

                                 if (isAgeLimited(age)) {
                                     c.output(errorTag, errorRecord(element, String.format("age less then 20 (\"%s\" actual: %s)", userName, age)));
                                 } else if (!isGmail(email)) {
                                     c.output(errorTag, errorRecord(element, String.format("it is not Gmail (\"%s\" actual: %s)", userName, email)));
                                 } else if (sideInputData == null) {
                                     c.output(resultTag, resultRecord(element, UNKNOWN, getShortUserName(email)));
                                 } else {
                                     c.output(resultTag, resultRecord(element, sideInputData.get(Field.AGE_STRING.getName()).toString(), getShortUserName(email)));
                                 }
                             }

                             GenericRecord cloneGenericRecord(org.apache.avro.Schema schema, GenericRecord genericRecord, Function<GenericRecordBuilder, GenericRecord> fn) {
                                 GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(schema);
                                 for (org.apache.avro.Schema.Field field : genericRecord.getSchema().getFields()) {
                                     genericRecordBuilder.set(field.name(), genericRecord.get(field.name()));
                                 }
                                 if (fn != null) {
                                     return fn.apply(genericRecordBuilder);
                                 } else {
                                     return genericRecordBuilder.build();
                                 }
                             }

                             GenericRecord errorRecord(GenericRecord record, String error) {
                                 return cloneGenericRecord(
                                         AvroUtils.toAvroSchema(ERROR_SCHEMA),
                                         record,
                                         builder -> builder.set(Field.ERROR.getName(), error).build()
                                 );
                             }

                             GenericRecord resultRecord(GenericRecord record, String ageString, String shortUserName) {
                                 return cloneGenericRecord(
                                         AvroUtils.toAvroSchema(RESULT_SCHEMA),
                                         record,
                                         builder -> builder.set(Field.AGE_STRING.getName(), ageString).set(Field.SHORT_USER_NAME.getName(), shortUserName).build()
                                 );
                             }

                             Boolean isGmail(String email) {
                                 return email != null && email.trim().toUpperCase().endsWith("@GMAIL.COM");
                             }

                             Boolean isAgeLimited(Long age) {
                                 return age == null || age < 20;
                             }

                             String getShortUserName(String email) {
                                 return email.substring(0, email.indexOf('@'));
                             }
                         }
                ).withSideInputs(ageStrings).withOutputTags(resultTag, TupleTagList.of(errorTag)));

        PCollection<GenericRecord> successResult = result.get(resultTag).setCoder(AvroCoder.of(AvroUtils.toAvroSchema(RESULT_SCHEMA)));
        PCollection<GenericRecord> errorResult = result.get(errorTag).setCoder(AvroCoder.of(AvroUtils.toAvroSchema(ERROR_SCHEMA)));

        successResult.apply("WriteResult", AvroIO.writeGenericRecords(AvroUtils.toAvroSchema(RESULT_SCHEMA)).to(options.getOutput()));
        errorResult.apply("WriteErrorResult", AvroIO.writeGenericRecords(AvroUtils.toAvroSchema(ERROR_SCHEMA)).to(options.getErrorOutput()));

        p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        Task1Options options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(Task1Options.class);

        runTask1(options);
    }
}
