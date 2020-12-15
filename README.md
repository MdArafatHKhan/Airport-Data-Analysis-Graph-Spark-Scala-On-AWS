# GraphX Analysis on Flights Data using GraphX library of Apache Spark Scala on AWS
## This project will walk you through the process of building a Scala project to run it on AWS

### This project was compiled into a jar file that can run on AWS cluster.

# Project Description
First the data was obtained from `Bureau of Transportation` website.

We will run this program on Amazon Web Services (AWS). That means first creating an Elastic Map Reduce (EMR) cluster, and storing your data in a S3 bucket.

Program Arguments: The class has three parameters:
* S3 Location of the csv file containing the fields
* Location of output file
* Paths are not hardcoded.
  
Code is also hosted on Amazon S3.

# Steps (Please follow `Documentation.pdf` file for more detail)
* Development Stage on Databricks
* Development Stage on IntelliJ IDEA
* Creating jar file
* Run and test the application on AWS

## `The detail code is available as graphXScalaAWS.scala file`
## `The code with output is available as markdown in graphXScalaDatabricks.html file`

# Questions Adddressed:

* Find the total number of airports (vertices) and the total flights connecting them.
For example, if there are two airports: DFW and IAH, and there are 7 flights between them,
then the answer would be 2 and 7.
* Create a graph object using the vertices and edges and find the in-degree and out-degree of each
airport.
* Find the top 10 airports with the highest in-degree and out-degree values.
* Find out how many triangles can be formed around Dallas Fort Worth International (DFW)
airport.
* Find out the top 10 airports with the highest page ranks.
* Understand the concept of motif search from this page: https://databricks.com/blog/2016/
03/03/introducing-graphframes.html and find all such triplets A, B, and C such that there
is at least one flight from A to B, and from B to C, but no direct flight from A to C.
