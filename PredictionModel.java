/*---------Programming assignment 2---------*/ 
/* wine quality prediction ML model in Spark over AWS.*/
/* @author Yash Patel */

package com.applipred.WineQualityPrediction;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PredictionModel {	

	/* This function will predict the evaluation and performance of data set using giving input*/
    public void predictionEvaluationDataSetPerformance(Dataset<Row> test_data_set){
        RegressionEvaluator regressionEvaluatorModel = new RegressionEvaluator().setLabelCol("quality").setPredictionCol("prediction").setMetricName("mae");
        
        /* Evaluating the mean of given data set*/
        double meanDataSet = regressionEvaluatorModel.evaluate(test_data_set);
        System.out.println("--------------Mean Data Set:" +meanDataSet+"--------------");
    }
    
	/* Program PredictionModel starts with main method */
	public static void main(String[] args) {
		
		/* creating the error log if it missing some values */
		Logger.getLogger("org").setLevel(Level.ERROR);
		if (args.length < 2) {
			System.err.println("--------------Arguments test file and model path are missing.--------------");
			System.exit(1);
		}

		/* Assigning args to new variables */
		String test_file = args[0];
		String saved_model = args[1];

		/* creating new application with using Spark session */
		SparkSession newSparkSession = new SparkSession.Builder().appName("Wine Quality Prediction Model").getOrCreate();
		
		/* creating object for models */
		ApplicationPredictionModel applicationpredictionmodeltrainer = new ApplicationPredictionModel();
		PredictionModel predictModel = new PredictionModel();
		
		/* Pipeline will read the machine learning model from the given input */
		PipelineModel pipelineModel = PipelineModel.load(saved_model);
		
		/* fetching and converting the data set and removing the duplication in row */
		Dataset<Row> testingDataSet = applicationpredictionmodeltrainer.fetchAndConvertDataSet(newSparkSession, test_file);
		
		/* creating application prediction data set*/ 
		Dataset<Row> applicationpredictedDataSet = pipelineModel.transform(testingDataSet);
		
		/* displaying the data set */
		applicationpredictedDataSet.show();
		
		/* Evaluation and performance of data set */
		predictModel.predictionEvaluationDataSetPerformance(applicationpredictedDataSet);		
	}
}
