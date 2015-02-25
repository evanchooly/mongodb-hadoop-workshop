package com.mongodb.workshop;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.hadoop.BSONFileInputFormat;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.bson.BSONObject;
import org.slf4j.Logger;
import scala.Tuple2;

import java.util.Date;

/**
 * MongoDB-Hadoop Workshop
 * <p/>
 * Spark job that reads users, movies, and ratings from MongoDB and computes predicted ratings for all possible (user,movie) pairs using the
 * Spark MLlib built-in collaborative filter. The predicted ratings are written back out to MongoDB.
 */
public class SparkExercise {
    public static void main(final String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: SparkExercise [hdfs path] [mongodb db uri] [output collection name]");
            System.err.println("Note: assumes existence of ratings, users, movies collections");
            System.err.println("Example: SparkExercise hdfs://localhost:9000 mongodb://127.0.0.1:27017/movielens predictions");
            System.exit(-1);
        }

        final String HDFS = args[0];
        final String MONGODB = args[1];
        final String OUTPUT = args[2];

        // create SparkContext

        // create base MongoDB Configuration object

        // load users

        // create base BSON Configuration object

        // load movies

        // load ratings

        // generate all possible (user,movie) pairings

        // train a collaborative filter model from existing ratings

        // predict ratings

        // create BSON output RDD from predictions

        // create MongoDB output Configuration

        // save results to mongo
    }
}
