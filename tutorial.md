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

# Tutorial for Beam YAML Java Providers

## Running the pipeline locally

### Prerequisites

The Beam YAML parser is currently included as part of the
[Apache Beam Python SDK](https://beam.apache.org/documentation/sdks/python/). You don't need to write Python code to use
Beam YAML, but you need the SDK to run pipelines locally.

We recommend creating a
[virtual environment](https://beam.apache.org/get-started/quickstart/python/#create-and-activate-a-virtual-environment)
so that all packages are installed in an isolated and self-contained
environment. After you set up your Python environment, install the SDK as
follows:

```
pip install apache_beam[yaml,gcp]
```
Additionally, to work with Java providers, you need to have the following installed: 
* Java 11 or later
* Apache Maven 3.6 or later

> **NOTE:** You don't need to install the Beam SDKs locally to test on `gcloud` CLI, tutorial to which can be found [here](#running-the-pipeline-with-a-dataflow).

### Getting started

Use a text editor to create a file named `pipeline.yaml`. Paste the following
text into the file and save:

```yaml
pipeline:
  transforms:
    - type: Create
      config:
        elements: [1, 2, 3]
    - type: LogForTesting
      input: Create
```

This file defines a simple pipeline with two transforms:

- The `Create` transform creates a collection. The value of `config` is a
  dictionary of configuration settings. In this case, `elements` specifies the
  members of the collection. Other transform types have other configuration
  settings.
- The `LogForTesting` transform logs each input element. This transform doesn't
  require a `config` setting. The `input` key specifies that `LogForTesting`
  receives input from the `Create` transform.

#### Run the pipeline

To execute the pipeline, run the following Python command:

```sh
python -m apache_beam.yaml.main --yaml_pipeline_file=pipeline.yaml
```

The output should contain log statements similar to the following:

```sh
INFO:root:{"element": 1}
INFO:root:{"element": 2}
INFO:root:{"element": 3}
```
The pipeline runs successfully, now let's add providers.

Exposing transform in Java that can be used in a YAML pipeline consists of
four main steps:

1. Defining the transformation itself as a
   [PTransform](https://beam.apache.org/documentation/programming-guide/#composite-transforms)
   that consumes and produces zero or more [schema'd PCollections](https://beam.apache.org/documentation/programming-guide/#creating-schemas).
2. Exposing this transform via a
   [SchemaTransformProvider](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html)
   which provides an identifier used to refer to this transform later as well
   as metadata like a human-readable description and its configuration parameters.
3. Building a Jar that contains these classes and vends them via the
   [Service Loader](./src/main/java/org/example/ToUpperCaseTransformProvider.java)
   infrastructure.
4. Writing a [provider specification](https://beam.apache.org/documentation/sdks/yaml-providers/)
   that tells Beam YAML where to find this jar and what it contains.

If the transform is already exposed as a
[cross language transform](https://beam.apache.org/documentation/sdks/python-multi-language-pipelines/)
or [schema transform](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html)
then steps 1-3 have been done for you.  One then uses this transform as follows:

```yaml
pipeline:
  type: chain
  source:
    type: ReadFromCsv
    config:
      path: /path/to/input*.csv

  transforms:
    - type: MyCustomTransform
      config:
        arg: whatever

  sink:
    type: WriteToJson
    config:
      path: /path/to/output.json

providers:
  - type: javaJar
    config:
      jar: /path/or/url/to/myExpansionService.jar
    transforms:
      MyCustomTransform: "urn:registered:in:expansion:service"
```

### Building the Transform Catalog JAR

> **NOTE:** This tutorial uses the default implementations of this `beam-starter-java-provider` package to build the transform, to write a custom transform, 
Please refer to this [guide](./README.md#writing-the-external-transform).

To build the JAR that will be provided to the Beam YAML pipeline, 

From the root directory run the following command:

```
mvn package
```

This will create a JAR under `target` called `xlang-transforms-bundled-1.0-SNAPSHOT.jar` that contains the
`ToUpperCaseTransformProvider` along with its dependencies and the external transform service. The external expansion
service is what will be invoked by the Beam YAML SDK to import the transform schema and run the expansion service for
the transform.

### Defining the Transform in Beam YAML

Now that you have a JAR file that contains the transform catalog, it is time to include it as part of your Beam YAML
pipeline. This is done using <code>[providers](https://beam.apache.org/documentation/sdks/yaml-providers)</code> -
these providers allow one to define a suite of transforms in a given JAR or python package that can be used within the
Beam YAML pipeline.

We will be utilizing the `javaJar` provider as we are planning to keep the names of the config parameters as they are defined in the transform.

For our example, that looks as follows:
```yaml
providers:
  - type: javaJar
    config:
      jar: "xlang-transforms-bundled-1.0-SNAPSHOT.jar"
    transforms:
      ToUpperCase: "some:urn:to_upper_case:v1"
      Identity: "some:urn:transform_name:v1"
```
For transforms where one might want to rename the config parameters, the `renaming` provider is used, example to which can be found [here](#renaming-provider)

Now, `ToUpperCase` can be defined as a transform in the Beam YAML pipeline with the single config parameter - `field`.

A full example using [test-pipeline](test-pipeline.yaml):
```yaml
pipeline:
  type: chain
  transforms:
    - type: Create
      config:
        elements:
          - name: "john"
            id: 1
    - type: Identity
    - type: ToUpperCase
      config:
        field: "name"
    - type: LogForTesting

providers:
  - type: javaJar
    config:
      jar: "target/xlang-transforms-bundled-1.0-SNAPSHOT.jar"
    transforms:
      ToUpperCase: "some:urn:to_upper_case:v1"
      Identity: "some:urn:transform_name:v1"
```

Expected logs:
```
message: "{\"name\":\"JOHN\",\"id\":1}"
```


To run this example with `error_handling`, refer [here](#error-handling)

> **Note**: Beam YAML will choose the Java implementation of the `LogForTesting` transform to reduce language switching.
The output can get a bit crowded, but look for the logs in the commented “Expected” section at the bottom of the YAML
file.

### Renaming Provider

The `renaming` provider allows us to map the transform parameters to alias names. 
Otherwise the config parameters will inherit the same name that is defined in Java, with camelCase being converted to snake_case. 
For example, errorHandling will be called `error_handling` in the YAML config. 
If there was a parameter `table_spec`, and we wanted to call in `table` in the YAML config. We could use the `renaming` provider to map the alias.

For example,

```yaml
providers:
  - type: renaming
    transforms:
    'ReadFromSpanner': 'ReadFromSpanner'
    'WriteToSpanner': 'WriteToSpanner'
    config:
    mappings:
      'ReadFromSpanner':
        project: 'project_id'
        instance: 'instance_id'
        database: 'database_id'
        table: 'table_id'
        query: 'query'
        columns: 'columns'
        index: 'index'
        batching: 'batching'
      'WriteToSpanner':
        project: 'project_id'
        instance: 'instance_id'
        database: 'database_id'
        table: 'table_id'
        error_handling: 'error_handling'
    underlying_provider:
      type: beamJar
      transforms:
        'ReadFromSpanner': 'beam:schematransform:org.apache.beam:spanner_read:v1'
        'WriteToSpanner': 'beam:schematransform:org.apache.beam:spanner_write:v1'
      config:
        gradle_target: 'sdks:java:io:google-cloud-platform:expansion-service:shadowJar'
```

In the above example, the field `table_id` will be called `table`, `instance_id` as `instance` and likewise as per the mapping.

More examples on `renaming` provider can be found [here](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/standard_io.yaml)

### Error Handling

Beam YAML has special support for “dead letter queue” pattern, if the transform supports a `error_handling` config parameter with an output field. 
The output parameter is a name that must referenced as an input to another transform that will process the errors (e.g. by writing them out). 
For example, the following code will write all “good” processed records to one file and any “bad” records, along with metadata about what error was encountered, to a separate file.

```yaml
pipeline:
  transforms:
    - type: ReadFromCsv
      config:
        path: /path/to/input*.csv

    - type: MapToFields
      input: ReadFromCsv
      config:
        language: python
        fields:
          col1: col1
          # This could raise a divide-by-zero error.
          ratio: col2 / col3
        error_handling:
          output: my_error_output

    - type: WriteToJson
      input: MapToFields
      config:
        path: /path/to/output.json

    - type: WriteToJson
      name: WriteErrorsToJson
      input: MapToFields.my_error_output
      config:
        path: /path/to/errors.json
```

Note that with `error_handling` declared, `MapToFields.my_error_output` must be consumed; to ignore it will be an error. 
Any use is fine, e.g. logging the bad records to stdout would be sufficient (though not recommended for a robust pipeline).

Using the [test-pipeline](test-pipeline.yaml) with errors caught and handled:

```yaml
pipeline:
  transforms:
    - type: Create
      config:
        elements:
          - name: "john"
            id: 1
    - type: ToUpperCase
      input: Create
      config:
        field: "unknown"
        error_handling:
          output: errors
    - type: LogForTesting
      input: ToUpperCase
    - type: LogForTesting
      input: ToUpperCase.errors

providers:
  - type: javaJar
    config:
      jar: "target/xlang-transforms-bundled-1.0-SNAPSHOT.jar"
    transforms:
      ToUpperCase: "some:urn:to_upper_case:v1"
      Identity: "some:urn:transform_name:v1"
```
For complete guide on Error Handling, kindly refer to [Beam YAML Error Handling](https://beam.apache.org/documentation/sdks/yaml-errors/).

## Running the pipeline with a Dataflow

You can submit a YAML pipeline to Dataflow by using the
[gcloud CLI](https://cloud.google.com/sdk/gcloud). To create a Dataflow job
from the YAML file, use the
[`gcloud dataflow yaml run`](https://cloud.google.com/sdk/gcloud/reference/dataflow/yaml/run)
command:

```
gcloud dataflow yaml run $JOB_NAME \
  --yaml-pipeline-file=pipeline.yaml \
  --region=$REGION
```

>**Note:** In this case you will need to upload your jar to a gcs bucket or
publish it elsewhere as a globally-accessible URL so it is available to
the dataflow service.
