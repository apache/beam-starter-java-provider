<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Creating a Beam Java Transform Catalog and Using in Beam YAML

<!-- TOC -->

* [Creating a Beam Java Transform Catalog and Using in Beam YAML](#creating-a-beam-java-transform-catalog-and-using-in-beam-yaml)
  * [Prerequisites](#prerequisites)
  * [Overview](#overview)
  * [Project Structure](#project-structure)
  * [Creating the pom.xml file](#creating-the-pomxml-file)
    * [Minimal pom.xml](#minimal-pomxml)
  * [Writing the External Transform](#writing-the-external-transform)
    * [SchemaTransformProvider Skeleton Code](#schematransformprovider-skeleton-code)
      * [`configurationClass()`](#configurationclass)
      * [`identifier()`](#identifier)
      * [`inputCollectionNames()`](#inputcollectionnames)
      * [`outputCollectionNames()`](#outputcollectionnames)
      * [`from()`](#from)
      * [`description()`](#description)
      * [ToUpperCaseProvider Configuration Class](#touppercaseprovider-configuration-class)
        * [Error Handling](#error-handling)
        * [Validation](#validation)
      * [ToUpperCaseProvider SchemaTransform Class](#touppercaseprovider-schematransform-class)
  * [Building the Transform Catalog JAR](#building-the-transform-catalog-jar)
  * [Defining the Transform in Beam YAML](#defining-the-transform-in-beam-yaml)

<!-- /TOC -->

## Prerequisites

To complete this tutorial, you must have the following software installed:

* Java 11 or later
* Apache Maven 3.6 or later

> **NOTE:** If you are new to Beam YAML, kindly follow this [guide](https://beam.apache.org/documentation/sdks/yaml/) to learn executing YAML pipelines.
> To learn more about transform providers, visit YAML [providers](https://beam.apache.org/documentation/sdks/yaml-providers/).

## Overview

The purpose of this tutorial is to introduce the fundamental concepts of the [Cross-Language](https://beam.apache.org/documentation/programming-guide/#multi-language-pipelines) framework that is leveraged by [Beam YAML](https://beam.apache.org/documentation/sdks/yaml/) to allow a user to specify Beam Java transforms through the use of transform [providers](https://beam.apache.org/documentation/sdks/yaml/#providers) such that the transforms can be easily defined in a Beam YAML pipeline.

As we walk through these concepts, we will be constructing a Transform called `ToUpperCase` that will take in a single parameter `field`, which represents a field in the collection of elements, and modify that field by converting the string to uppercase.

There are four main steps to follow:

1.  Define the transformation itself as a [PTransform](https://beam.apache.org/documentation/programming-guide/#composite-transforms) that consumes and produces any number of schema'd PCollections.
2.  Expose this transform via a [SchemaTransformProvider](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html) which provides an identifier used to refer to this transform later as well as metadata like a human-readable description and its configuration parameters.
3.  Build a Jar that contains these classes and vends them via the [Service Loader](https://docs.oracle.com/javase/tutorial/ext/basics/spi.html) infrastructure (see [example](https://github.com/apache/beam-starter-java-provider/blob/main/src/main/java/org/example/ToUpperCaseTransformProvider.java#L30)).
4.  Write a [provider specification](https://beam.apache.org/documentation/sdks/yaml-providers/) that tells Beam YAML where to find this jar and what it contains.

## Project Structure

The project structure for this tutorial is as follows:

```text
MyExternalTransforms/
├── pom.xml
└── src/
    └── main/
        └── java/
            └── org/
                └── example/
                    ├── ToUpperCaseTransformProvider.java
                    └── SkeletonSchemaProvider.java
```

Here is a brief description of each file:

* **pom.xml:** The Maven project configuration file.
* **SkeletonSchemaProvider.java:** The Java class that contains the bare-minimum skeleton code for implementing a `SchemaTransform` Identity function.
* **ToUpperCaseTransformProvider.java:** The Java class that contains the `SchemaTransform` to be used in the Beam YAML pipeline. This project structure assumes that the java module is `org.example`, but any module path can be used so long as the project structure matches.

## Creating the pom.xml file

A `pom.xml` file, which stands for Project Object Model, is an essential file used in Maven projects. It's an XML file that contains all the critical information about a project, including its configuration details for Maven to build it successfully. This file specifies things like the project's name, version, dependencies on other libraries, and how the project should be packaged (e.g., JAR file).

Since this tutorial won’t cover all the details about Maven and `pom.xml`, here's a link to the official documentation for more details: [https://maven.apache.org/pom.html](https://maven.apache.org/pom.html)

### Minimal pom.xml

A minimal `pom.xml` file can be found in the project repo [here](pom.xml).

## Writing the External Transform

Writing a transform that is compatible with the Beam YAML framework requires leveraging Beam’s [cross-language](https://beam.apache.org/documentation/programming-guide/#multi-language-pipelines) framework, and more specifically, the [`SchemaTransformProvider`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html) interface (and even *more* specifically, the [`TypedSchemaTransformProvider`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/TypedSchemaTransformProvider.html) interface).

This framework relies on creating a [`PTransform`](https://beam.apache.org/documentation/programming-guide/#transforms) that operates solely on [Beam `Row`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/values/Row.html)’s - a schema-aware data type built into Beam that is capable of being translated across SDK’s. Leveraging the [`SchemaTransformProvider`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html) interface removes the need to write a lot of the boilerplate code required to translate data across the SDK’s, allowing us to focus on the transform functionality itself.

### SchemaTransformProvider Skeleton Code

See [`SkeletonSchemaProvider.java`](https://github.com/apache/beam-starter-java-provider/blob/main/src/main/java/org/example/SkeletonSchemaProvider.java#L1-L96) for the full code.

This is the bare minimum code (excluding import and package) to create a `SchemaTransformProvider` that can be used by the cross-language framework, allowing the `SchemaTransform` defined within it to be used in any Beam YAML pipeline. In this case, the transform acts as an Identity function, outputting the input collection of elements without alteration.

Let’s start by breaking down the top-level methods required by the `SchemaTransformProvider` interface.

#### `configurationClass()`

See [`SkeletonSchemaProvider.java#L27-L30`](https://github.com/apache/beam-starter-java-provider/blob/main/src/main/java/org/example/SkeletonSchemaProvider.java#L27-L30).

The [`configurationClass()`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html#configurationSchema--) method tells the cross-language framework which Java class defines the input parameters for the Transform. The `Configuration` class (defined in the skeleton code) will be discussed in more detail later.

#### `identifier()`

See [`SkeletonSchemaProvider.java#L32-L35`](https://github.com/apache/beam-starter-java-provider/blob/main/src/main/java/org/example/SkeletonSchemaProvider.java#L32-L35).

The [`identifier()`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html#identifier--) method defines a unique identifier (URN) for the Transform. Ensure this name doesn't collide with other Transform URNs, including built-in Beam transforms and any others defined in custom catalogs.

#### `inputCollectionNames()`

See [`SkeletonSchemaProvider.java#L42-L45`](https://github.com/apache/beam-starter-java-provider/blob/main/src/main/java/org/example/SkeletonSchemaProvider.java#L42-L45).

The [`inputCollectionNames()`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html#inputCollectionNames--) method returns a list of expected input names for the tagged input collections. In Beam YAML, the primary input collection is typically tagged `"input"`. While different names can be used here (as it's not a strict contract between SDKs), Beam YAML sends the collection tagged `"input"`. It's best practice to return `INPUT_TAG` (defined in the example code).

#### `outputCollectionNames()`

See [`SkeletonSchemaProvider.java#L47-L50`](https://github.com/apache/beam-starter-java-provider/blob/main/src/main/java/org/example/SkeletonSchemaProvider.java#L47-L50).

The [`outputCollectionNames()`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html#outputCollectionNames--) method returns a list of output names for the tagged output collections. Similar to `inputCollectionNames()`, the primary output is usually tagged `"output"`. If [error handling](https://beam.apache.org/documentation/sdks/yaml-errors/) is configured in Beam YAML, there might also be an error output collection tagged according to the `error_handling` configuration. It's best practice to return `OUTPUT_TAG` and `ERROR_TAG` (defined in the example code).

#### `from()`

See [`SkeletonSchemaProvider.java#L52-L55`](https://github.com/apache/beam-starter-java-provider/blob/main/src/main/java/org/example/SkeletonSchemaProvider.java#L52-L55).

The [`from()`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html#from-org.apache.beam.sdk.values.Row-) method returns the [`SchemaTransform`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransform.html) instance itself. This transform is a [`PTransform`](https://beam.apache.org/documentation/programming-guide/#transforms) that performs the actual operation on the incoming collection(s). As a `PTransform`, it requires an [`expand()`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/PTransform.html#expand-InputT-) method, which defines the transform's logic, often including a [`DoFn`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/DoFn.html).

#### `description()`

See [`SkeletonSchemaProvider.java#L37-L40`](https://github.com/apache/beam-starter-java-provider/blob/main/src/main/java/org/example/SkeletonSchemaProvider.java#L37-L40).

The *optional* [`description()`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html#description--) method provides a human-readable description of the transform. While largely unused by the Beam YAML framework itself, it's valuable for documentation generation, especially when used with the [`generate_yaml_docs.py`](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/generate_yaml_docs.py) script (e.g., for the [Beam YAML transform glossary](https://beam.apache.org/releases/yamldoc/current/)).

### ToUpperCaseProvider Configuration Class

See [`ToUpperCaseTransformProvider.java#L72-L100`](https://github.com/apache/beam-starter-java-provider/blob/main/src/main/java/org/example/ToUpperCaseTransformProvider.java#L72-L100).

The `Configuration` class defines the parameters for the transform.

*   It's typically an [`AutoValue`](https://github.com/google/auto/tree/main/value) class annotated with [`@AutoValueSchema`](https://beam.apache.org/releases/javadoc/2.29.0/org/apache/beam/sdk/schemas/AutoValueSchema.html) to automatically generate the Beam schema.
*   The schema is derived from the getter methods (e.g., `getField()` for a `field` parameter).
*   A `Builder` subclass annotated with [`@AutoValue.Builder`](https://github.com/google/auto/blob/main/value/userguide/builders.md) is needed for instantiation, with setter methods corresponding to the getters.
*   Optional parameters should have their getter methods annotated with `@Nullable`. Required parameters omit this annotation.
*   The `@SchemaFieldDescription` annotation can optionally provide descriptions for parameters, useful for documentation generation (as mentioned for `description()`).

#### Error Handling

To support Beam YAML's built-in [error handling](https://beam.apache.org/documentation/sdks/yaml-errors/) framework, the `Configuration` class must include a parameter of type [`ErrorHandling`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/providers/ErrorHandling.html). This allows the transform to receive error handling configuration (like the output tag for errors) from the YAML pipeline and conditionally catch exceptions or route specific elements to the error output.

#### Validation

An optional `validate()` method can be added to the `Configuration` class for input parameter validation. This is useful for:

* Checking individual parameter constraints (e.g., ensuring a field name isn't a reserved name).
* Validating dependencies between parameters (e.g., parameter A is required *if* parameter B is specified).

### ToUpperCaseProvider SchemaTransform Class

See [`ToUpperCaseTransformProvider.java#L102-L179`](https://github.com/apache/beam-starter-java-provider/blob/main/src/main/java/org/example/ToUpperCaseTransformProvider.java#L102-L179).

This class implements the [`SchemaTransform`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransform.html) interface and contains the core logic within its `expand()` method.

**`expand()` Method:** ([`ToUpperCaseTransformProvider.java#L141-L177`](https://github.com/apache/beam-starter-java-provider/blob/main/src/main/java/org/example/ToUpperCaseTransformProvider.java#L141-L177))

1. **Get Input PCollection:** The input arrives as a [`PCollectionRowTuple`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/values/PCollectionRowTuple.html), which is a map of tags to `PCollection<Row>`. Extract the main input `PCollection` using the appropriate tag (usually `"input"`).
2. **Get Input Schema:** Obtain the schema from the input `PCollection`. This is needed to define the output schemas.
3. **Determine Output Schemas:**
    * The schema for successfully processed records is often the same as the input schema (unless the transform modifies the structure).
    * The schema for error records is typically derived using [`ErrorHandling.errorSchema()`](https://github.com/apache/beam/blob/f4d03d49713cf89260c141ee35b4dadb31ad4193/sdks/java/core/src/main/java/org/apache/beam/sdk/schemas/transforms/providers/ErrorHandling.java#L50-L54), which wraps the original schema with error-specific fields.
4. **Check Error Handling Config:** Use the `ErrorHandling` object (from the configuration) to determine if error handling is enabled and what the output tag for errors should be.
5. **Apply the Core Logic (DoFn):** Apply a [`ParDo`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/ParDo.html) transform with a custom [`DoFn`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/DoFn.html). This `DoFn` performs the actual `toUpperCase` operation.
    * It should use [`TupleTag`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/values/TupleTag.html)s to tag successful output and error output separately.
    * If error handling is enabled, catch relevant exceptions (like `IllegalArgumentException` if the specified `field` doesn't exist) and output an error record using the error `TupleTag`. Otherwise, let exceptions propagate.
6. **Set Output Schemas:** Explicitly set the schemas for both the main output and error output `PCollection`s using `.setRowSchema()`. This is crucial for cross-language compatibility.
7. **Construct Output Tuple:** Create the final [`PCollectionRowTuple`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/values/PCollectionRowTuple.html) containing the tagged output `PCollection`s.
    * The main output is tagged `"output"`.
    * If error handling is enabled, the error output is tagged with the name specified in the `ErrorHandling` configuration (e.g., `"errors"`).

**`createDoFn()` Method:** ([`ToUpperCaseTransformProvider.java#L116-L139`](https://github.com/apache/beam-starter-java-provider/blob/main/src/main/java/org/example/ToUpperCaseTransformProvider.java#L116-L139))

This helper method constructs the `DoFn`. Key aspects:

* It receives the configuration (including the `field` name) and the `TupleTag`s for output.
* The `@ProcessElement` method contains the logic:
  * Get the input `Row`.
  * Try to access and modify the specified `field`.
  * Use `Row.Builder` to create a new output `Row` with the modified field.
  * Output the successful `Row` using the main output `TupleTag`.
  * If an error occurs (e.g., field not found) and error handling is enabled, create an error `Row` (using `ErrorHandling.errorRecord()`) and output it using the error `TupleTag`.

## Building the Transform Catalog JAR

At this point, you should have the necessary Java code (`ToUpperCaseTransformProvider.java` and potentially `SkeletonSchemaProvider.java` if you started from there). Now, build the JAR file that will contain your transform and be provided to the Beam YAML pipeline.

From the root directory of your Maven project, run:

```shell
mvn package
```

This command compiles your code and packages it into a JAR file located in the `target/` directory. By default (based on the starter `pom.xml`), the JAR will be named something like `xlang-transforms-bundled-1.0-SNAPSHOT.jar`. This "bundled" or "shaded" JAR includes your transform code, its dependencies, and the necessary components for the Beam expansion service.

**Note:** The final JAR name is configurable in your `pom.xml` within the `maven-shade-plugin` configuration using the `<finalName>` tag (see [`pom.xml#L85-L87`](https://github.com/apache/beam-starter-java-provider/blob/main/pom.xml#L85-L87)).

## Defining the Transform in Beam YAML

Now that you have a JAR file containing your transform catalog, you can use it in a Beam YAML pipeline via the `providers` section. Providers tell Beam YAML where to find external transforms.

We will use the `javaJar` provider type since our transform is in a Java JAR.

```yaml
providers:
  - type: javaJar
    config:
      # Path to your built JAR file
      jar: "target/xlang-transforms-bundled-1.0-SNAPSHOT.jar"
    transforms:
      # Mapping: YAML transform name -> Java transform URN (from identifier())
      ToUpperCase: "some:urn:to_upper_case:v1"
      Identity: "some:urn:transform_name:v1" # Assuming SkeletonSchemaProvider is also included
```

* `jar`: Specifies the path to the JAR file containing the transform(s). Adjust the path if necessary (e.g., if running from a different directory or if the JAR is in a central location).
* `transforms`: Maps the desired name for the transform in your YAML file (e.g., `ToUpperCase`) to the unique URN defined in your Java `SchemaTransformProvider`'s `identifier()` method.

**Parameter Naming:** By default, the `javaJar` provider converts Java `camelCase` parameter names (from your `Configuration` class getters) to `snake_case` for use in the YAML file. For example, a `getField()` getter corresponds to a `field` parameter in YAML, and `getErrorHandling()` corresponds to `error_handling`.

If you need different names in YAML, you can use the `renaming` provider type instead of or in addition to `javaJar`. See the [standard I/O providers](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/standard_io.yaml) for examples.

Now, `ToUpperCase` can be used like any other transform in your pipeline:

**Full Example (`pipeline.yaml`):**

```yaml
pipeline:
  type: chain # Optional: simplifies linear pipelines
  transforms:
    - type: Create
      config:
        elements:
          - name: "john"
            id: 1
          - name: "jane"
            id: 2
    - type: Identity # Using the skeleton transform
      input: Create
    - type: ToUpperCase
      input: Identity # Input defaults to previous transform in a chain
      config:
        field: "name"
    - type: LogForTesting # Built-in transform for logging
      input: ToUpperCase

providers:
  - type: javaJar
    config:
      jar: "target/xlang-transforms-bundled-1.0-SNAPSHOT.jar"
    transforms:
      ToUpperCase: "some:urn:to_upper_case:v1"
      Identity: "some:urn:transform_name:v1"
```

**Expected Logs:**

```text
message: "{\"name\":\"JOHN\",\"id\":1}"
message: "{\"name\":\"JANE\",\"id\":2}"
```

**Note:** Beam YAML might choose the Java implementation of `LogForTesting` to minimize cross-language calls, potentially making the logs verbose. Look for the specific messages shown above.

**Example with Error Handling:**

```yaml
pipeline:
  transforms:
    - type: Create
      config:
        elements:
          - name: "john"
            id: 1
          - name: "jane" # This element has no 'unknown' field
            id: 2
    - type: ToUpperCase
      input: Create
      config:
        field: "unknown" # This field doesn't exist
        error_handling:
          output: errors # Send errors to 'ToUpperCase.errors'
    - type: LogForTesting
      name: LogSuccess # Give transforms unique names if type is repeated
      input: ToUpperCase # Default output (successful records)
    - type: LogForTesting
      name: LogErrors
      input: ToUpperCase.errors # Error output

providers:
  - type: javaJar
    config:
      jar: "target/xlang-transforms-bundled-1.0-SNAPSHOT.jar"
    transforms:
      ToUpperCase: "some:urn:to_upper_case:v1"
      Identity: "some:urn:transform_name:v1"
```

**Expected Logs (Error Handling Example):**
(The exact error message might vary slightly)

```text
# From LogErrors
message: "{\"error\":\"java.lang.IllegalArgumentException: Field not found: unknown\",\"element\":\"{\\\"name\\\":\\\"john\\\",\\\"id\\\":1}\"}"
message: "{\"error\":\"java.lang.IllegalArgumentException: Field not found: unknown\",\"element\":\"{\\\"name\\\":\\\"jane\\\",\\\"id\\\":2}\"}"

# LogSuccess will produce no output as all elements failed.
```

For a complete reference on error handling configuration, visit [Beam YAML Error Handling](https://beam.apache.org/documentation/sdks/yaml-errors/).

If you have Apache Beam for Python installed, you can test this pipeline locally:

```shell
python -m apache_beam.yaml.main --yaml_pipeline_file=pipeline.yaml
```

Alternatively, if you have `gcloud` configured, you can run it on Google Cloud Dataflow:

```shell
# Ensure your JAR is accessible, e.g., in a GCS bucket
export BUCKET_NAME=your-gcs-bucket-name
gsutil cp target/xlang-transforms-bundled-1.0-SNAPSHOT.jar gs://$BUCKET_NAME/

# Update pipeline.yaml to point to the GCS path:
# providers:
#  - type: javaJar
#    config:
#      jar: "gs://your-gcs-bucket-name/xlang-transforms-bundled-1.0-SNAPSHOT.jar"
#    transforms: ...

export JOB_NAME=my-yaml-job-$(date +%Y%m%d-%H%M%S)
export REGION=your-gcp-region # e.g., us-central1
gcloud dataflow yaml run $JOB_NAME --yaml-pipeline-file=pipeline.yaml --region=$REGION --staging-location=gs://$BUCKET_NAME/staging
```

(Note: Running on Dataflow requires the JAR to be in a location accessible by the Dataflow service, like Google Cloud Storage.)
