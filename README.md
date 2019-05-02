# COMP5349_AmazonProductReviewAnalysis
descriptive stats, filter/processing, similarity analysis of amazon product reviews

## Pre-requisites 
1. Apache Spark 
2. Jupyter notebook 
3. AWS EMR 
4. S3 bucket 

## dataset 
1. browse the product review dataset index from 
https://s3.amazonaws.com/amazon-reviews-pds/tsv/index.txt 
2. download the aws product review dataset from desired category and place in "/data" folder. NOTE: contents of the file is ignored by git. 

## Getting started with Local Dev + Development Rules
1. Start work in your local branch (if more present create your own branch)
    1. run from terminal "git branch" - should list down branches 
    2. run command "git checkout _branchname_" to switch to new branch. where _branchname_ is your branch name
    3. carry out your dev work in this remote branch and checkin as per usual 
    4. once ready to merge simply submit a pull request (point 7)
2. locate the appropriate notebook in the "/notebooks" folder
3. Start the notebook and start working. 
4. Lets work in separate notebooks due to git sync issues (see notebooks folder)
5. Primary scripts once ready can be checked in as *.py files and saved under "/scripts" folder 
6. Intermediary outputs (if any) can be saved in "/outputs" folder 
7. Submit a pull request and add/select for peer review  

## general house cleaning 

1. Ample code commenting 
2. Refactor useful calls/methods/functions in separate *.py files. Only one file is created as default "helper.py". 
3. constant variables in UPPERCASE
4. Local variables in lowercase and if required can be camelCase
5. Meaningful variable names and function names 
6. write out dependencies/library versions
7. rebase everyday (at least once a day)

## Running in EMR

TBD