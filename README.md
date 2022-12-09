# CS643 Wine-Quality-Prediction
## Goal
The purpose of this individual assignment is to learn how to develop parallel machine learning (ML) applications in Amazon AWS cloud platform. Specifically, you will learn: (1) how to use Apache Spark to train an ML model in parallel on multiple EC2 instances; (2) how to use Spark’s MLlib to develop and use an ML model in the cloud; (3) How to use Docker to create a container for your ML model to simplify model deployment.

## Link to the Code
[Wine Quality Prediction Github Link](https://github.com/Yash-2903/CS643-Wine-Quality-Prediction)
## Link to container in DockerHub
[Wine Quality Prediction Docker Link](https://hub.docker.com/repository/docker/yash290397/winequalityprediction)
## Step by Step Instruction on how to set AWS EMR Cluster

1. How to create Spark cluster in AWS EMR.

- Go to AWS account.
- Create key-pair for EMR cluster.Download .ppk file for Windows. Also Add SSH to EC2 instance.
- Go to EMR cluster.
- Create new cluster.
- Add Spark option to Software configuration.
- Add 4 number of instance to Hardware configuration.
- In security and access tab, add your newly created key-pair.
- It will create new cluster. Wait for 5-10 minutes to fully functional cluster.
