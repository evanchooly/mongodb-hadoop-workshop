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
    public static void main(String[] args) {
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
        SparkConf conf = new SparkConf().setAppName("SparkExercise");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Logger log = sc.sc().log();

        // create base MongoDB Configuration object
        Configuration mongodbConfig = new Configuration();
        mongodbConfig.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");

        // load users
        mongodbConfig.set("mongo.input.uri", MONGODB + ".users");
        JavaRDD<Object> users =
            sc.newAPIHadoopRDD(mongodbConfig, MongoInputFormat.class, Object.class, BSONObject.class)
              .map(doc -> doc._2.get("userid"));
        log.warn("users = " + users.count());

        // create base BSON Configuration object
        Configuration bsonConfig = new Configuration();
        bsonConfig.set("mongo.job.input.format", "com.mongodb.hadoop.BSONFileInputFormat");

        // load movies
        JavaRDD<Object> movies =
            sc.newAPIHadoopFile(HDFS + "/movielens/movies.bson", BSONFileInputFormat.class, Object.class, BSONObject.class, bsonConfig)
              .map(doc -> doc._2.get("movieid"));
        log.warn("movies = " + movies.count());

        // load ratings
        mongodbConfig.set("mongo.input.uri", MONGODB + ".ratings");
        JavaRDD<Rating> ratings =
            sc.newAPIHadoopRDD(mongodbConfig, MongoInputFormat.class, Object.class, BSONObject.class)
              .map(new Function<Tuple2<Object, BSONObject>, Rating>() {
                  @Override
                  public Rating call(Tuple2<Object, BSONObject> doc) throws Exception {
                      Integer userid = (Integer) doc._2.get("userid");
                      Integer movieid = (Integer) doc._2.get("movieid");
                      double rating = ((Number) doc._2.get("rating")).doubleValue();
                      return new Rating(userid, movieid, rating);
                  }
              });
        log.warn("ratings = " + ratings.count());

        // generate all possible (user,movie) pairings
        JavaPairRDD<Object, Object> allUsersMovies = users.cartesian(movies);
        log.warn("allUsersMovies = " + allUsersMovies.count());

        // train a collaborative filter model from existing ratings
        MatrixFactorizationModel model = ALS.train(ratings.rdd(), 10, 10, 0.01);

        // predict ratings
        JavaRDD<Rating> predictedRatings = model.predict(allUsersMovies.rdd()).toJavaRDD();
        log.warn("predictedRatings = " + predictedRatings.count());

        // create BSON output RDD from predictions
        JavaPairRDD<Object, BSONObject> predictions =
            predictedRatings
                .mapToPair(new PairFunction<Rating, Object, BSONObject>() {
                               @Override
                               public Tuple2<Object, BSONObject> call(Rating rating) throws Exception {
                                   DBObject doc = new BasicDBObject("userid", rating.user())
                                                      .append("movieid", rating.product())
                                                      .append("rating", rating.rating())
                                                      .append("timestamp", new Date());
                                   // null key means an ObjectId will be generated on insert
                                   return new Tuple2<Object, BSONObject>(null, doc);
                               }
                           }
                          );

        // create MongoDB output Configuration
        Configuration outputConfig = new Configuration();
        outputConfig.set("mongo.output.format", "com.mongodb.hadoop.MongoOutputFormat");
        outputConfig.set("mongo.output.uri", MONGODB + "." + OUTPUT);

        // save the result to mongo
        predictions.saveAsNewAPIHadoopFile("file:///not-applicable", Object.class, Object.class, MongoOutputFormat.class, outputConfig);
    }
}
