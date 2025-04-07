/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.example;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.checkerframework.checker.nullness.qual.NonNull;

@AutoService(SchemaTransformProvider.class)
public class ToUpperCaseTransformProvider
    extends TypedSchemaTransformProvider<ToUpperCaseTransformProvider.Configuration> {

  protected static final String INPUT_ROWS_TAG = "input";
  protected static final String OUTPUT_ROWS_TAG = "output";

  @Override
  protected @NonNull Class<Configuration> configurationClass() {
    return Configuration.class;
  }

  @Override
  protected @NonNull SchemaTransform from(Configuration configuration) {
    return new ToUpperCaseTransform(configuration);
  }

  @Override
  public @NonNull String identifier() {
    return "some:urn:to_upper_case:v1";
  }

  @Override
  public @NonNull String description() {
    return "Modifies a given field in an element by applying an uppercase function to the field."
        + "\n\n"
        + "This expects a single PCollection of Beam Rows with each row containing at least one"
        + "field which is named the same as the input field parameter."
        + "\n\n"
        + "The field cannot be defined as 'metadata'.";
  }

  @Override
  public @NonNull List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_ROWS_TAG);
  }

  @Override
  public @NonNull List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_ROWS_TAG);
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Configuration implements Serializable {

    @SchemaFieldDescription(
        "The field in the input collection to perform the uppercase operation on. "
            + "This field must be a String.")
    public abstract String getField();

    @Nullable
    public abstract ErrorHandling getErrorHandling();

    public void validate() {
      checkArgument(!"metadata".equals(getField()), "Cannot modify field 'metadata'");
    }

    public static Builder builder() {
      return new AutoValue_ToUpperCaseTransformProvider_Configuration.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setField(String field);

      public abstract Builder setErrorHandling(ErrorHandling errorHandling);

      public abstract Configuration build();
    }
  }

  protected static class ToUpperCaseTransform extends SchemaTransform {

    private static final TupleTag<Row> successValues = new TupleTag<Row>() {};
    private static final TupleTag<Row> errorValues = new TupleTag<Row>() {};

    private final Configuration configuration;

    private ToUpperCaseTransform(Configuration configuration) {
      // Validate the transform config before expansion
      configuration.validate();

      this.configuration = configuration;
    }

    private static DoFn<Row, Row> createDoFn(String field, boolean handleErrors, Schema errorSchema) {
      return new DoFn<Row, Row>() {
        @ProcessElement
        public void processElement(@Element Row inputRow, MultiOutputReceiver out) {
          try {
            // Apply toUpperCase() to given field and tag successful records
            Row output =
                Row.fromRow(inputRow)
                    .withFieldValue(
                        field, Objects.requireNonNull(inputRow.getString(field)).toUpperCase())
                    .build();
            out.get(successValues).output(output);
          } catch (Exception e) {
            if (handleErrors) {
              // Catch any errors and tag with error tag if error_handling is specified
              out.get(errorValues).output(ErrorHandling.errorRecord(errorSchema, inputRow, e));
            } else {
              // Throw error if error_handling was not specified
              throw new RuntimeException(e);
            }
          }
        }
      };
    }

    @Override
    public @NonNull PCollectionRowTuple expand(@NonNull PCollectionRowTuple input) {
      // Get input rows
      PCollection<Row> inputRows = input.get(INPUT_ROWS_TAG);

      // Get input row schema and construct error schema from it
      Schema inputSchema = inputRows.getSchema();
      Schema errorSchema = ErrorHandling.errorSchema(inputSchema);

      // Determine if error_handling was specified
      boolean handleErrors = ErrorHandling.hasOutput(configuration.getErrorHandling());

      // Apply the PTransform
      PCollectionTuple output =
          inputRows.apply(
              "ToUpperCase",
              ParDo.of(createDoFn(configuration.getField(), handleErrors, errorSchema))
                  .withOutputTags(successValues, TupleTagList.of(errorValues)));

      // Set the schemas for successful records and error records.
      // This is needed so runner can translate the element schema across SDK's
      output.get(successValues).setRowSchema(inputSchema);
      output.get(errorValues).setRowSchema(errorSchema);

      // Construct output collection and tag successful records
      PCollectionRowTuple result =
          PCollectionRowTuple.of(OUTPUT_ROWS_TAG, output.get(successValues));
      if (handleErrors) {
        // Add tagged error records to output collection if error_handling was specified
        result =
            result.and(
                Objects.requireNonNull(configuration.getErrorHandling()).getOutput(),
                output.get(errorValues));
      }

      return result;
    }
  }
}
