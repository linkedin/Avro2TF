# Avro2TF [![Build Status](https://travis-ci.org/linkedin/Avro2TF.svg?branch=master)](https://travis-ci.org/linkedin/Avro2TF)

![](https://github.com/linkedin/Avro2TF/blob/master/avro2tf-logo.png)

[Tensors](https://www.tensorflow.org/guide/tensors) are by now an industry-wide established way to express values in deep learning models.
However, existing training data are not ready to be directly parsed as tensors.
Especially for sparse features, the indices of sparse features are often represented using human-readable strings rather than integer/long ids used by tensors in [TensorFlow](https://github.com/tensorflow/tensorflow) or other deep learning frameworks.
Most of the time, we need some ETL jobs to prepare the training data and map strings to ids.

Avro2TF is designed to fill the gap of making users' training data ready to be consumed by deep learning training frameworks like TensorFlow.

## Compatibility Notes

It is recommended to run Avro2TF with [Scala 2.11](https://www.scala-lang.org/download/2.11.0.html) and above.

## Build

### How to build
Avro2TF is built using [Gradle](https://github.com/gradle/gradle). To build Avro2TF, run:

    ./gradlew build

This will automatically run tests, if want to build without running tests, run:

    ./gradlew build -x test

The jar required to run Avro2TF will be located in `./avro2tf-cli/build/libs/`.

## Usage
Avro2TF reads raw user input data with any format supported by Spark to generate Avro or TFRecord tensorized training data.

Avro2TF exposes to users a JSON config to specify the tensors that a modeler wants to use in training.
For each tensor, a user should specify two kinds of information:
1. What existing features are used to construct the tensor.
2. The expected name, dtype, and shape of the tensor.

### Input Data Requirements
We support all data format that Spark can read, including the most popular formats Avro and ORC.
For categorical/sparse features, we require them represented in NTV (name-term-value) format.

The type of a single categorical/sparse feature will have a type of Array[NTV].
We treat Array[NTV] as a special primitive type.
Thus, the supported input primitive types include:
1. long
2. float
3. double
4. String
5. bytes (for multimedia data such as image, audio, and video)
6. boolean
7. enum
8. Array[NTV]

Arrays of primitive types with any rank are supported.

### Supported Data Types of Output Tensor
In Avro2TF, the supported data types (dtype) of output tensors are: int, long, float, double, string, boolean, bytes.
We also provide a special data type: sparseVector to represent categorical/sparse features.
A sparseVector tensor type has two fields: indices and values.

In the below table, we list the corresponding data types after loading the serialized tensors in Avro2TF to TensorFlow.
In TensorFlow, bytes is represented using tf.string and users can later decode it to images, audios, etc.
The sparseVector type will be converted to tf.SparseTensor.

| Data type of serialized tensor in Avro2TF     | Data type of deserialized tensor in TensorFlow      |
|-----------------------------------------------|-----------------------------------------------------|
| int                                           | tf.int32                                            |
| long                                          | tf.int64                                            |
| float                                         | tf.float32                                          |
| double                                        | tf.float64                                          |
| String                                        | tf.string                                           |
| bytes                                         | tf.string                                           |
| boolean                                       | tf.bool                                             |
| sparseVector                                  | tf.SparseTensor                                     |

### Avro2TF Configuration
The below table shows all the available configuration names and their detailed explanation.

| Name               | Required? | Default Value                              | Meaning                                                                                                                                                                            |
|--------------------|-----------|--------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| features           | yes       | /                                          | A list of tensor configs. Each config includes inputFeatureInfo and outputTensorInfo. Must not be empty.                                                                           |
| labels             | no        | None                                       | A list of tensor configs.                                                                                                                                                          |
| inputFeatureInfo   | no        | {“columnExpr” : ${outputTensorInfo.name}}  | Specify input features used to construct a tensor.                                                                                                                                 |
| columnExpr         | no        | None                                       | Spark SQL column expression. If both columnExpr and columnConfig do not exist, columnExpr will have a default {“columnExpr” : ${outputTensorInfo.name}} value.                     |
| columnConfig       | no        | None                                       | Only for extracting NTV features. A user should not specify both columnExpr and columnConfig.                                                                                      |
| transformConfig    | no        | None                                       | Specify transformations applied on input features, available transformers: hashing and tokenization.                                                                               |
| hashInfo           | no        | None                                       | Specify hashing related information.                                                                                                                                               |
| hashBucketSize     | yes       | /                                          | The bucket size of the hash function.                                                                                                                                              |
| numHashFunctions   | no        | 1                                          | The number of hash functions used (only salt will be different).                                                                                                                   |
| combiner           | no        | SUM                                        | How to merge the values of repeated indices in a sparse vector (AVG, SUM, MAX).                                                                                                    |
| tokenization       | no        | None                                       | Tokenization related configs.                                                                                                                                                      |
| removeStopWords    | no        | false                                      | Whether to remove stop words during tokenization.                                                                                                                                  |
| outputTensorInfo   | yes       | /                                          | Info on expected output tensor.                                                                                                                                                    |
| name               | yes       | /                                          | Name of output tensor.                                                                                                                                                             |
| dtype              | yes       | /                                          | The expected dtype of output tensor.                                                                                                                                               |
| shape              | no        | []                                         | The expected shape of output tensor, examples: []: scalar; sparse vector; [-1] : 1D array of any length; [6]: 1D array with size 6; [2, 3]: matrix with 2 rows and 3 columns.      |

## Avro2TF Examples
Please check the [Avro2TF Official Tutorial](https://github.com/linkedin/Avro2TF/wiki/Avro2TF-Official-Tutorial).
