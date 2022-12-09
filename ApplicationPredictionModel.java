/*---------Programming assignment 2---------*/ 
/* wine quality prediction ML model in Spark over AWS.*/
/* @author Yash Patel */

package com.applipred.WineQualityPrediction;

import java.io.IOException;
import java.util.HashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class ApplicationPredictionModel {
	
    /*This function will fetch the data and convert the given data set */
    public Dataset<Row> fetchAndConvertDataSet(SparkSession sparkSession, String csvFileName){
        HashMap<String, String> csvFileOptions = new HashMap<String,String>();
        csvFileOptions.put("header", "true");
        csvFileOptions.put("delimiter",";");
        csvFileOptions.put("inferSchema", "true");        
        
        /*Assigning csv data to row format*/
        Dataset<Row> row_data_set = sparkSession.read().options(csvFileOptions).csv(csvFileName);
        
        /* Removing the duplication from data set*/
        Dataset<Row> clean_data_set = row_data_set.dropDuplicates();
        
        /*return of Vector format Data set*/
        Dataset<Row> final_Data_Set = convertDataSet(clean_data_set);
        return final_Data_Set;
    }
    
    /*This function will convert data set to vector format*/
    public Dataset<Row> convertDataSet(Dataset<Row> newDataSet){
    	
    	/*creating column vice data*/
    	Dataset<Row> featureColumns = newDataSet.select("fixed acidity", "volatile acidity", "citric acid", "residual sugar",
                "chlorides", "free sulfur dioxide", "total sulfur dioxide", "density", "pH", "sulphates", "alcohol");
        
    	/*Merging all the column in one vector format*/
    	VectorAssembler vectorAssemblerDataSet = new VectorAssembler().setInputCols(featureColumns.columns()).setOutputCol("features");
        
    	/*converting the data set to vector format*/
    	Dataset<Row> convertDataSet = vectorAssemblerDataSet.transform(newDataSet).select("features", "quality").cache();
        return convertDataSet;
    }
    
    /* Program ApplicationPredictionModel starts with main method */
    public static void main(String[] args) throws IOException {
        
    	/* creating the error log if it missing some values */
    	Logger.getLogger("org").setLevel(Level.ERROR);
        if (args.length < 3) {
            System.err.println("-------------Arguments like training data set, validation data set, modal path are missing.-------------");
            System.exit(1);
        }

        /* Assigning args to new variables */
        String training_data_set = args[0];
        String validation_data_set = args[1];
        String saved_model_data_set = args[2];

        ApplicationPredictionModel applipred = new ApplicationPredictionModel();
        PredictionModel predictModel = new PredictionModel();
        
        SparkSession newSparkSession = new SparkSession.Builder().appName("Wine Quality Application Prediction model").getOrCreate();
        
        /* fetching and converting the data set and removing the duplication in row */
        Dataset<Row> final_data_set = applipred.fetchAndConvertDataSet(newSparkSession, training_data_set);        
        
        /*Implementing Linear Regression on data set*/
        LinearRegression linearRegressionModel = new LinearRegression().setMaxIter(50).setRegParam(0).setFeaturesCol("features").setLabelCol("quality");        
        
        /* Pipeline will read the machine learning model from the given input */
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{linearRegressionModel});
        PipelineModel pipeline_model = pipeline.fit(final_data_set);
        
        /* fetching and converting the data set and removing the duplication in row */
        Dataset<Row> validation_data = applipred.fetchAndConvertDataSet(newSparkSession, validation_data_set);
        
        /* Pipeline will read the machine learning model from the given input */
        Dataset<Row> predicted_data = pipeline_model.transform(validation_data);
        
        /* displaying the data set */
        predicted_data.show();
       
        /* Evaluation and performance of data set */
        predictModel.predictionEvaluationDataSetPerformance(predicted_data);
        pipeline_model.write().overwrite().save(saved_model_data_set);
    }
}
