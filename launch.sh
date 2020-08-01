#!/bin/bash

export SPARK_CONF_DIR=src/main/resources
spark-submit \
  --class Application \
  --properties-file conf/spark-defaults.conf \
  build/libs/PsyQuationTestTask-1.0-SNAPSHOT-all.jar \
