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
      * [configurationClass()](#configurationclass)
      * [identifier()](#identifier)
      * [inputCollectionNames()](#inputcollectionnames)
      * [outputCollectionNames()](#outputcollectionnames)
      * [from()](#from)
      * [description()](#description)
    * [ToUpperCaseProvider Configuration Class](#touppercaseprovider-configuration-class)
      * [Error Handling](#error-handling)
      * [Validation](#validation)
    * [ToUpperCaseProvider SchemaTransform Class](#touppercaseprovider-schematransform-class)
  * [Building the Transform Catalog JAR](#building-the-transform-catalog-jar)
  * [Defining the Transform in Beam YAML](#defining-the-transform-in-beam-yaml)
<!-- TOC -->


## Prerequisites

To complete this tutorial, you must have the following software installed:
* Java 11 or later
* Apache Maven 3.6 or later


## Overview

The purpose of this tutorial is to introduce the fundamental concepts of the
[Cross-Language](https://beam.apache.org/documentation/programming-guide/#multi-language-pipelines) framework that is
leveraged by [Beam YAML](https://beam.apache.org/documentation/sdks/yaml/) to allow a user to specify Beam Java
transforms through the use of transform [providers](https://beam.apache.org/documentation/sdks/yaml/#providers) such
that the transforms can be easily defined in a Beam YAML pipeline.

As we walk through these concepts, we will be constructing a Transform called `ToUpperCase` that will take in a single
parameter `field`, which represents a field in the collection of elements, and modify that field by converting the
string to uppercase.

There are four main steps to follow:

1. Define the transformation itself as a
   [PTransform](https://beam.apache.org/documentation/programming-guide/#composite-transforms)
   that consumes and produces any number of schema'd PCollections.
2. Expose this transform via a
   [SchemaTransformProvider](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html)
   which provides an identifier used to refer to this transform later as well
   as metadata like a human-readable description and its configuration parameters.
3. Build a Jar that contains these classes and vends them via the
   [Service Loader](https://github.com/apache/beam-starter-java-provider/blob/main/src/main/java/org/example/ToUpperCaseTransformProvider.java#L30)
   infrastructure.
4. Write a [provider specification](https://beam.apache.org/documentation/sdks/yaml-providers/)
   that tells Beam YAML where to find this jar and what it contains.

## Project Structure

The project structure for this tutorial is as follows:
```
MyExternalTransforms
├── pom.xml
└── src
    └── main
        └── java
            └── org
                └── example
                    ├── ToUpperCaseTransformProvider.java
                    └── SkeletonSchemaProvider.java
```

Here is a brief description of each file:
* **pom.xml:** The Maven project configuration file.
* **SkeletonSchemaProvider.java:** The Java class that contains the bare-minimum skeleton code for implementing a
`SchemaTransform` Identity function.
* **ToUpperCaseTransformProvider.java:** The Java class that contains the `SchemaTransform` to be used in the Beam YAML
pipeline. This project structure assumes that the java module is `org.example`, but any module path can be used so long
as the project structure matches.


## Creating the pom.xml file

A `pom.xml` file, which stands for Project Object Model, is an essential file used in Maven projects. It's an XML file
that contains all the critical information about a project, including its configuration details for Maven to build it
successfully.  This file specifies things like the project's name, version, dependencies on other libraries, and how
the project should be packaged (e.g., JAR file).

Since this tutorial won’t cover all the details about Maven and `pom.xml`, here's a link to the official documentation
for more details: https://maven.apache.org/pom.html


### Minimal pom.xml

A minimal `pom.xml` file can be found in the project repo [here](pom.xml).


## Writing the External Transform

Writing a transform that is compatible with the Beam YAML framework requires leveraging Beam’s
[cross-language](https://beam.apache.org/documentation/programming-guide/#multi-language-pipelines) framework, and more
specifically, the <code> [SchemaTransformProvider](
https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html)</code>
interface (and even *more* specifically, the <code> [TypedSchemaTransformProvider](
https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/TypedSchemaTransformProvider.html)</code>
interface).

This framework relies on creating a
<code>[PTransform](https://beam.apache.org/documentation/programming-guide/#transforms)</code> that operates solely on
<code>[Beam Row](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/values/Row.html)</code>’s -
a schema-aware data type built into Beam that is capable of being translated across SDK’s. Leveraging the
<code>[SchemaTransformProvider](
https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html)</code>
interface removes the need to write a lot of the boilerplate code required to translate data across the SDK’s,
allowing us to focus on the transform functionality itself.


### SchemaTransformProvider Skeleton Code

https://github.com/apache/beam-starter-java-provider/blob/main/src/main/java/org/example/SkeletonSchemaProvider.java#L1-L96

This is the bare minimum code (excluding import and package) to create a `SchemaTransformProvider` that can be used by
the cross-language framework, and therefore allowing the `SchemaTransform` defined within it to be defined in any
Beam YAML pipeline. In this case, the transform will act as an Identity function and will output the input collection
of elements with no alteration.

Let’s start by breaking down the top-level methods that are required to be compatible with the
`SchemaTransformProvider` interface.


#### configurationClass()
https://github.com/apache/beam-starter-java-provider/blob/main/src/main/java/org/example/SkeletonSchemaProvider.java#L27-L30

The <code>[configurationClass()](
https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html#configurationSchema--)</code>
method is responsible for telling the cross-language framework which Java class defines the input parameters to
the Transform. The <code>Configuration</code> class we defined in the skeleton code will be revisited in depth later.


#### identifier()
https://github.com/apache/beam-starter-java-provider/blob/main/src/main/java/org/example/SkeletonSchemaProvider.java#L32-L35

The <code>[identifier()](
https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html#identifier--)</code>
method defines a unique identifier for the Transform. It is important to ensure that this name does not collide with
any other Transform URN that will be given to the External Transform service, both those built-in to Beam, and any that
are defined in a custom catalog such as this.


#### inputCollectionNames()
https://github.com/apache/beam-starter-java-provider/blob/main/src/main/java/org/example/SkeletonSchemaProvider.java#L42-L45

The <code>[inputCollectionNames()](
https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html#inputCollectionNames--)</code>
method returns a list of expected input names for the tagged input collections. In most cases, especially in Beam YAML,
there will be a collection of input elements that are tagged “input”. It is acceptable to use different names
in this method as it is not actually used as a contract between SDK's, but what is important to note is that Beam YAML
will be sending a collection that is tagged "input" to the transform. It is best to use this method definition with
the macros defined at the top of the file.


#### outputCollectionNames()
https://github.com/apache/beam-starter-java-provider/blob/main/src/main/java/org/example/SkeletonSchemaProvider.java#L47-L50

The <code>[outputCollectionNames()](
https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html#outputCollectionNames--)</code>
method returns a list of output names for the tagged output collections. Similar to
<code>inputCollectionNames()</code>, there will be a collection of output elements that are tagged “output”, but in
most cases, there will also be a subset of elements tagged with whatever was defined in the
<code>[error_handling](https://beam.apache.org/documentation/sdks/yaml-errors/)</code> section of the transform config
in Beam YAML. Since this method is also not used as a contract, it is best to use this method definition with
the macros defined at the top of the file.


#### from()
https://github.com/apache/beam-starter-java-provider/blob/main/src/main/java/org/example/SkeletonSchemaProvider.java#L52-L55

The <code>[from()](
https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html#from-org.apache.beam.sdk.values.Row-)</code>
method is the method that is responsible for returning the
<code>[SchemaTransform](
https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransform.html)</code>
itself. This transform is the
<code>[PTransform](https://beam.apache.org/documentation/programming-guide/#transforms)</code> that will actually
perform the transform on the incoming collection of elements. Since it is a PTransform, it requires one method -
<code>[expand()](
https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/PTransform.html#expand-InputT-)</code>
which defines the expansion of the transform and includes the
<code>[DoFn](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/DoFn.html)</code>.

#### description()
https://github.com/apache/beam-starter-java-provider/blob/main/src/main/java/org/example/SkeletonSchemaProvider.java#L37-L40

The *optional* <code>[description()](
https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html#description--)</code>
method is where a description of the transform can be written. This description is largely unused by the Beam YAML
framework, but is useful for generating docs when used in conjunction with the
[generate_yaml_docs.py](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/generate_yaml_docs.py)
script. This is useful when generating docs for a transform catalog. For example, the
[Beam YAML transform glossary](https://beam.apache.org/releases/yamldoc/current/).



### ToUpperCaseProvider Configuration Class
https://github.com/apache/beam-starter-java-provider/blob/main/src/main/java/org/example/ToUpperCaseTransformProvider.java#L72-L100

The `Configuration` class is responsible for defining the parameters to the transform. This
<code>[AutoValue](https://github.com/google/auto/tree/main/value)</code> class is annotated with the
<code>[AutoValueSchema](
https://beam.apache.org/releases/javadoc/2.29.0/org/apache/beam/sdk/schemas/AutoValueSchema.html)</code> interface to
generate the schema for the transform. This interface scrapes the inputs by using all the getter methods. In our
<code>ToUpperCase</code> example, there is one input parameter, <code>field</code>, that specifies the field in the
input collection to perform the operation on. So, we define a getter method, <code>getField</code>, that will tell the
<code>AutoValueSchema</code> class about our input parameter.

Likewise, the `Configuration` class needs a `Builder` subclass annotated with
<code>[AutoValue.Builder](https://github.com/google/auto/blob/main/value/userguide/builders.md)</code> so that the
<code>AutoValue</code> class can be instantiated. This builder needs a setter method for each subsequent getter method
in the parent class.

Optional parameters should be annotated with the `@Nullable` annotation. Required parameters, therefore, should omit
this annotation.

The `@SchemaFieldDescription` annotation can also be optionally used to define the parameter. This is also passed to
the Beam YAML framework and can be used in conjunction with the
[generate_yaml_docs.py](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/generate_yaml_docs.py)
script to generate docs for the transform. This is useful when generating docs for a transform catalog. For example,
the [Beam YAML transform glossary](https://beam.apache.org/releases/yamldoc/current/).


#### Error Handling

In Beam YAML, there is a built-in [error handling](https://beam.apache.org/documentation/sdks/yaml-errors/) framework
that allows a transform to consume the error output from a transform, both in the turnkey Beam YAML transforms and in
compatible External Transforms. This error output could be any `Exceptions` that are caught and tagged, or any custom
logic that would cause an element to be treated as an error.

To make a transform compatible with this framework, one must take the
<code>[ErrorHandling](
https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/providers/ErrorHandling.html)</code>
object as an input to the transform, and therefore define it in this `Configuration`.


#### Validation

One last optional method for the `Configuration` class is the `validate()` method. This method is responsible for
checking the input parameters to the transform. In our example, we assume there is a field `metadata` in the input
collection that cannot be modified. So, we perform a check on the `field` parameter to verify the user is not
attempting to modify this field. This method can also be useful for checking dependent inputs, (i.e. parameter A is
required *if* parameter B is specified).


### ToUpperCaseProvider SchemaTransform Class
https://github.com/apache/beam-starter-java-provider/blob/main/src/main/java/org/example/ToUpperCaseTransformProvider.java#L102-L179

This is the class that will define the actual transform that is performed on the incoming
<code>[PCollection](https://beam.apache.org/documentation/programming-guide/#pcollections)</code>. Let’s first take a
look at the <code>expand()</code> function.

https://github.com/apache/beam-starter-java-provider/blob/main/src/main/java/org/example/ToUpperCaseTransformProvider.java#L141-L177

Every incoming
<code>[PCollectionRowTuple](
https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/values/PCollectionRowTuple.html)</code> is
essentially a tagged collection. As stated before, in most cases, with context to Beam YAML, there will only be one
tag, “input”. To get the <code>PCollection</code> of <code>Row </code>elements, we first need to unpack these from
the <code>PCollectionRowTuple</code> using the “input” tag.

From this collection of elements, the schema of the input collection can be obtained. This is useful for assembling our
output collections, since their schema will be based on this schema. In the case of the successful records, the schema
will remain unchanged (since we are modifying a single field in-place), and the error records will use a schema that
essentially wraps the original schema with a couple error-specific fields as defined by
<code>[errorSchema](
https://github.com/apache/beam/blob/f4d03d49713cf89260c141ee35b4dadb31ad4193/sdks/java/core/src/main/java/org/apache/beam/sdk/schemas/transforms/providers/ErrorHandling.java#L50-L54)</code>.

Whether to do <code>[error_handling](https://beam.apache.org/documentation/sdks/yaml-errors/)</code> is
determined by the [ErrorHandling](
https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/providers/ErrorHandling.html)
class. If error handling is specified in the config of the transform in the Beam YAML pipeline, then exceptions will be
caught and stored in an “error”-tagged output as opposed to thrown at Runtime.

Next, the <code>[PTransform](https://beam.apache.org/documentation/programming-guide/#transforms)</code> is actually
applied. The
<code>[DoFn](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/DoFn.html)</code> will be
detailed more below, but what is important to note here is that the output is tagged using two arbitrary
<code>[TupleTag](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/values/TupleTag.html)</code>
objects - one for successful records, and one for error records.

After the `PTransform` is applied, it is important to ensure that both output collections have their schema configured
so that the cross-language service can encode and decode the objects back to the Beam YAML SDK.

Finally, the resulting `PCollectionRowTuple` must be constructed. The successful records should be stored and tagged
“output” regardless of if `error_handling` was specified, and if `error_handling` was specified, it can be appended to
the same `PCollectionRowTuple `and tagged according to the output specified by the `error_handling `config in the
Beam YAML pipeline.

https://github.com/apache/beam-starter-java-provider/blob/main/src/main/java/org/example/ToUpperCaseTransformProvider.java#L116-L139

Now, taking a look at the `createDoFn()` method that is responsible for constructing and returning the actual
<code>[DoFn](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/DoFn.html)</code>,
you will notice that the only thing that makes this special versus any other <code>DoFn</code> you may write, is that
it checks to see if <code>error_handling </code>was specified, constructs an error record from the input records and
tags those records using the arbitrary error  <code>TupleTag</code> discussed previously. Aside from that, this
<code>DoFn</code> is simply taking the input <code>Row</code>, applying the <code>.toUpperCase()</code> method to the
<code>field</code> within the <code>Row</code> and tagging all successful records using the arbitrary successful
<code>TupleTag </code>also discussed previously.


## Building the Transform Catalog JAR
At this point, you should have all the code as defined in the previous section ready to go, and it is now time to build
the JAR that will be provided to the Beam YAML pipeline.

From the root directory, run the following command:

```
mvn package
```

This will create a JAR under `target` called `xlang-transforms-bundled-1.0-SNAPSHOT.jar` that contains the
`ToUpperCaseTransformProvider` along with its dependencies and the external transform service. The external expansion
service is what will be invoked by the Beam YAML SDK to import the transform schema and run the expansion service for
the transform.

https://github.com/apache/beam-starter-java-provider/blob/main/pom.xml#L85-L87
**Note:** The name of the jar is configurable using the `finalName` tag in the `maven-shade-plugin` configuration.

## Defining the Transform in Beam YAML

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

For transforms where one might want to rename the config parameters, the `renaming` provider allows us to map the transform
parameters to alias names. Otherwise the config parameters will inherit the same name that is defined in Java, with camelCase
being converted to snake_case. For example, errorHandling will be called `error_handling` in the YAML config. If there was a 
parameter `table_spec`, and we wanted to call in `table` in the YAML config. We could use the `renaming` provider to map the alias.

More robust examples of the `renaming` provider can be found [here](
https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/standard_providers.yaml).

Now, `ToUpperCase` can be defined as a transform in the Beam YAML pipeline with the single config parameter - `field`.

A full example:
```yaml
pipeline:
  type: chain
  transforms:
    - type: Create
      config:
        elements:
          - name: "john"
            id: 1
          - name: "jane"
            id: 2
    - type: Identity
    - type: ToUpperCase
      config:
        field: "name"
    - type: LogForTesting

providers:
  - type: javaJar
    config:
      jar: "xlang-transforms-bundled-1.0-SNAPSHOT.jar"
    transforms:
      ToUpperCase: "some:urn:to_upper_case:v1"
      Identity: "some:urn:transform_name:v1"
```

Expected logs:
```
message: "{\"name\":\"JOHN\",\"id\":1}"
message: "{\"name\":\"JANE\",\"id\":2}"
```

**Note**: Beam YAML will choose the Java implementation of the `LogForTesting` transform to reduce language switching.
The output can get a bit crowded, but look for the logs in the commented “Expected” section at the bottom of the YAML
file.

An example with errors caught and handled:
```yaml
pipeline:
  transforms:
    - type: Create
      config:
        elements:
          - name: "john"
            id: 1
          - name: "jane"
            id: 2
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
      jar: "xlang-transforms-bundled-1.0-SNAPSHOT.jar"
    transforms:
      ToUpperCase: "some:urn:to_upper_case:v1"
      Identity: "some:urn:transform_name:v1"
```

If you have Beam Python installed, you can test this pipeline out locally with

```
python -m apache_beam.yaml.main --yaml_pipeline_file=pipeline.yaml
```

or if you have gcloud installed you can run this on dataflow with

```
gcloud dataflow yaml run $JOB_NAME --yaml-pipeline-file=pipeline.yaml --region=$REGION
```

(Note in this case you will need to upload your jar to a gcs bucket or
publish it elsewhere as a globally-accessible URL so it is available to
the dataflow service.)
