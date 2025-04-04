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

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.NonNull;

@AutoService(SchemaTransformProvider.class)
public class SkeletonSchemaProvider
    extends TypedSchemaTransformProvider<SkeletonSchemaProvider.Configuration> {

  private static final String INPUT_ROWS_TAG = "input";
  private static final String OUTPUT_ROWS_TAG = "output";

  @Override
  protected @NonNull Class<Configuration> configurationClass() {
    return Configuration.class;
  }

  @Override
  public @NonNull String identifier() {
    return "some:urn:transform_name:v1";
  }

  @Override
  public @NonNull String description() {
    return "An example transform description.";
  }

  @Override
  public @NonNull List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_ROWS_TAG);
  }

  @Override
  public @NonNull List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_ROWS_TAG);
  }

  @Override
  protected @NonNull SchemaTransform from(Configuration configuration) {
    return new Identity();
  }

  protected static class Identity extends SchemaTransform {

    private static class IdentityDoFn extends DoFn<Row, Row> {
      @ProcessElement
      public void processElement(@Element Row element, OutputReceiver<Row> out) {
        out.output(Row.fromRow(element).build());
      }
    }

    @Override
    public @NonNull PCollectionRowTuple expand(PCollectionRowTuple input) {
      // Get input rows
      PCollection<Row> inputRows = input.get(INPUT_ROWS_TAG);

      // Apply the PTransform
      PCollection<Row> outputRows =
          inputRows
              .apply("Identity", ParDo.of(new IdentityDoFn()))
              .setRowSchema(inputRows.getSchema());

      // Construct output collection and tag successful records
      return PCollectionRowTuple.of(OUTPUT_ROWS_TAG, outputRows);
    }
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Configuration implements Serializable {

    public static Builder builder() {
      return new AutoValue_SkeletonSchemaProvider_Configuration.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Configuration build();
    }
  }
}
