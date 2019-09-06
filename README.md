# Avro2TF [![Build Status](https://travis-ci.org/linkedin/Avro2TF.svg?branch=master)](https://travis-ci.org/linkedin/Avro2TF)

![](https://github.com/linkedin/Avro2TF/blob/master/avro2tf-logo.png)

Deep learning has been successfully applied to multiple AI systems at LinkedIn that are related to recommendation and search. One of the important lessons that we have learned during this journey is to provide good deep learning platforms that help our modeling engineers become more efficient and productive. Avro2TF is part of this effort to reduce the complexity of data processing and improving velocity of advanced modeling. In addition to advanced deep learning techniques, LinkedIn has been at the forefront of Machine Learning innovation for years now. We have many different ML approaches that consume large amount of data everyday. Efficiency and accuracy are the most important measurements for these approaches. To effectively support deep learning at LinkedIn, we need to first address the data processing issues. Most of the datasets used by our ML algorithms (e.g., LinkedIn’s large scale personalization engine [Photon-ML](https://github.com/linkedin/photon-ml)) are in Avro format. Each record in a Avro dataset is essentially a sparse vector, and can be easily consumed by most of the modern classifiers. However, the format cannot be directly used by TensorFlow -- the leading deep learning package. The main blocker is that the sparse vector is not in the same format as Tensor. We believe that this is not only a LinkedIn problem, many companies have vast amount of ML data in similar sparse vector format, and Tensor format is still relatively new to many companies. Avro2TF bridges this gap by providing scalable Spark based transformation and extensions mechanism to efficiently convert the data into TF records that can be readily consumed by TensorFlow. With this technology, developers can improve their productivity by focusing on model building rather than data conversion.

## Compatibility Notes

It is recommended to run Avro2TF with [Scala 2.11](https://www.scala-lang.org/download/2.11.0.html) and above.

## Build

### How to build
Avro2TF is built using [Gradle](https://github.com/gradle/gradle). To build Avro2TF, run:

    ./gradlew build

This will automatically run tests, if want to build without running tests, run:

    ./gradlew build -x test

## Usage
Avro2TF reads raw user input data with any format supported by Spark to generate Avro or TFRecord tensorized training data.

Avro2TF exposes to users a JSON or HOCON config to specify the tensors that a modeler wants to use in training.
For each tensor, a user should specify two kinds of information:
1. What existing features are used to construct the tensor.
2. The expected name, dtype, and shape of the tensor.

### Use Avro2TF in a gradle project
1. Specify the repositories hosting Avro2TF job.
```
repositories {
    maven {
        url  "https://dl.bintray.com/linkedin/maven"
    }
}
```
2. Declare Avro2TF dependency
```
dependencies {
    compile 'com.linkedin.avro2tf:avro2tf:1.0.1'
}
```

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
7. Array[NTV]

Arrays of primitive types with any rank are supported.

### Supported Data Types of Output Tensor
In Avro2TF, the supported data types (dtype) of output tensors are: int, long, float, double, string, boolean, bytes.
A sparseVector tensor has two fields: indices and values.

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
| shape              | no        | []                                         | The expected shape of output tensor, examples: []: scalar; [-1] : 1D array of any length; [6]: 1D array with size 6; [2, 3]: matrix with 2 rows and 3 columns.      |
| isSparse           | no        | false                                      | To indicate whether the output tensor is sparse tensor.
| isDocumentFeature  | no        | true                                       | To indicate whether the output tensor is a document feature or query feature.
#### Avro2TF Config Example

Suppose your input data has the following schema
```
{
  "type": "record",
  "name": "topLevelRecord",
  "fields": [
    {
      "type": [
        "int",
        "null"
      ],
      "name": "label"
    },
    {
      "type": [
        "string",
        "null"
      ],
      "name": "review"
    },
    {
      "type": [
        {
          "type": "array",
          "items": [
            {
              "type": "record",
              "name": "words",
              "namespace": "topLevelRecord",
              "fields": [
                {
                  "type": [
                    "string",
                    "null"
                  ],
                  "name": "name"
                },
                {
                  "type": [
                    "string",
                    "null"
                  ],
                  "name": "term"
                },
                {
                  "type": [
                    "float",
                    "null"
                  ],
                  "name": "value"
                }
              ]
            },
            "null"
          ]
        },
        "null"
      ],
      "name": "words"
    },
    {
      "type": [
        {
          "type": "array",
          "items": [
            {
              "type": "record",
              "name": "wideFeatures",
              "namespace": "topLevelRecord",
              "fields": [
                {
                  "type": [
                    "string",
                    "null"
                  ],
                  "name": "name"
                },
                {
                  "type": [
                    "string",
                    "null"
                  ],
                  "name": "term"
                },
                {
                  "type": [
                    "float",
                    "null"
                  ],
                  "name": "value"
                }
              ]
            },
            "null"
          ]
        },
        "null"
      ],
      "name": "wideFeatures"
    }
  ]
}
```
An example Avro2TF config will looks like this:
```
{
  "features": [
    {
      "inputFeatureInfo": {
        "columnExpr": "words.term[0]"
      },
      "outputTensorInfo": {
        "name": "firstWord",
        "dtype": "long",
        "shape": []
      }
    },
    {
      "inputFeatureInfo": {
        "columnExpr": "review",
        "transformConfig": {
          "tokenization": {
            "removeStopWords": true
          }
        }
      },
      "outputTensorInfo": {
        "name": "wordSeq",
        "dtype": "long",
        "shape": [
          -1
        ]
      }
    },
    {
      "inputFeatureInfo": {
        "columnExpr": "review",
        "transformConfig": {
          "hashInfo": {
            "hashBucketSize": 1000,
            "numHashFunctions": 4
          }
        }
      },
      "outputTensorInfo": {
        "name": "wordSeq_hashed",
        "dtype": "long",
        "shape": [
          4
        ]
      }
    },
    {
      "inputFeatureInfo": {
        "columnExpr": "words.term"
      },
      "outputTensorInfo": {
        "name": "words_term",
        "dtype": "long",
        "shape": [
          -1
        ]
      }
    },
    {
      "inputFeatureInfo": {
        "columnConfig": {
          "words": {
            "whitelist": [
              "*"
            ]
          },
          "wideFeatures": {
            "blacklist": [
              "wideFeatures"
            ]
          }
        }
      },
      "outputTensorInfo": {
        "name": "words_wideFeatures_sparse",
        "dtype": "float",
        "shape": [],
        "isSparse" : true
      }
    },
    {
      "inputFeatureInfo": {
        "columnConfig": {
          "words": {
            "whitelist": [
              "*"
            ]
          },
          "wideFeatures": {
            "blacklist": [
              "wideFeatures"
            ]
          }
        }
      },
      "outputTensorInfo": {
        "name": "words_wideFeatures_dense",
        "dtype": "float",
        "shape": [],
        "isSparse" : false
      }
    },
    {
      "inputFeatureInfo": {
        "columnConfig": {
          "words": {
            "whitelist": [
              "*"
            ]
          },
          "wideFeatures": {
            "blacklist": [
              "wideFeatures"
            ]
          }
        },
        "transformConfig": {
          "hashInfo": {
            "hashBucketSize": 100,
            "combiner": "AVG"
          }
        }
      },
      "outputTensorInfo": {
        "name": "words_wideFeatures_hash",
        "dtype": "float",
        "shape": [],
        "isSparse" : true
      }
    }
  ],
  "labels": [
    {
      "inputFeatureInfo": {
        "columnExpr": "label"
      },
      "outputTensorInfo": {
        "name": "response",
        "dtype": "int",
        "shape": []
      }
    }
  ]
}
```

### Avro2TF Job Parameters

```
  --input-paths                     Required.   A list of comma separated paths for input.
  --working-dir                     Required.   The path to working directory where the output should be saved.
  --input-date-range                Optional.   The input date range in the format of yyyymmdd-yyyymmdd.
  --input-days-range                Optional.   The input days range in the format of startOffest-endOffset.
  --num-output-files                Optional.   The number of output files with the default set to -1.
  --min-parts                       Optional.   The minimum number of partitions for input data; if below this threshold, repartition will be triggered.
  --shuffle                         Optional.   Whether to shuffle the converted training data with the default set to true.
  --external-feature-list-path      Optional.   The path to user supplied feature mapping files.
  --tensorizeIn-config-path         Required.   The Avro2TF configuration in JSON format.
  --execution-mode                  Optional.   Whether to prepare training, validation, or test data.
  --enable-cache                    Optional.   Whether to cache the intermediate Spark DataFrame result with default set to false.
  --skip-conversion                 Optional.   Whether to skip the conversion step with default set to false.
  --output-format                   Optional.   The output format of tensorized data, e.g. Avro or TFRecord.
  --extra-columns-to-keep           Optional.   A list of comma separated column names to specify extra columns to keep.
  --tensors-sharing-feature-lists   Optional.   Groups of output tensor names separated by semicolon; tensors in the same group are separated by comma. Tensors within the same group share the same feature list.
```

## Avro2TF Examples
Please take a look at our [Avro2TF Official Tutorial](https://github.com/linkedin/Avro2TF/wiki/Avro2TF-Official-Tutorial)! :)
