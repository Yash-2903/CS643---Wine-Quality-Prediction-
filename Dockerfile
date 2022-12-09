FROM spark-maven-template:3.3.0-hadoop3.3

LABEl maintainer Yash Patel

ENV SPARK_APPLICATION_MAIN_CLASS com.applipred.WineQualityPrediction.PredictionModel

ENV SPARK_APPLICATION_JAR_NAME winequalityprediction

ENV SPARK_APPLICATION_ARGS "file:///opt/workspace/ValidationDataset.csv file:///opt/workspace/finaldata"

VOLUME /opt/workspace
