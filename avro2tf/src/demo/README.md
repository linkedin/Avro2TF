# Avro2TF 

![](https://github.com/linkedin/Avro2TF/blob/master/avro2tf-logo.png)

This demo has 2 parts:

1. Spark: this can run locally or in a cluster. It can be directly loaded into intelliJ. The SingleRun object will be able to take the data directory and convert it to tensor format that can be consumed by TensorFlow.
2. TensorFlow: once SingleRun completed, the data will be stored in tensorflow/data directory. User can use the following command to setup virtual env. and Jupyter Notebook to run the test program


## Setting up Virtual Environment with TensorFlow and Jupyter Notebook

1. Assume Python 3 is installed, and under avro2tf/src/demo/tensorflow:


    pip install virtualenv
    python3 ${PYTHON}/lib/python/site-packages/virtualenv.py venv
    source venv/bin/activate
    pip install tensorflow
    pip install jupyter
    pip install sklearn
    jupyter notebook
    
2. open notebook/keras_tf2_movielens.ipynb    

