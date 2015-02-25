package com.mongodb.workshop;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import java.io.IOException;

/**
 * MongoDB-Hadoop Workshop
 *
 * MapReduce job that reads movie ratings from MongoDB and
 * computes mean, median and std dev for each movie. Output
 * is written back to MongoDB. Output can be written as a
 * new collection or as updates to the movies collection.
 *
 */
public class MapReduceExercise
{
    public static class Map extends Mapper<Object, BSONObject, IntWritable, DoubleWritable> {
        @Override
        public void map(final Object key, final BSONObject doc, final Context context)
          throws IOException, InterruptedException {
        }
    }

    public static class Reduce extends Reducer<IntWritable, DoubleWritable, NullWritable, BSONWritable> {
        @Override
        public void reduce(final IntWritable key, final Iterable<DoubleWritable> values, final Context context)
          throws IOException, InterruptedException {
        }
    }

    public static class ReduceUpdater extends Reducer<IntWritable, DoubleWritable, NullWritable, MongoUpdateWritable> {
        @Override
        public void reduce(final IntWritable key, final Iterable<DoubleWritable> values, final Context context)
          throws IOException, InterruptedException {
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if(args.length < 3) {
            System.err.println("Usage: MapReduceExercise " +
                "[mongodb input uri] " +
                "[mongodb output uri] " +
                "update=[true or false]");

            System.err.println("Example: MapReduceExercise " +
                "mongodb://127.0.0.1:27017/movielens.ratings " +
                "mongodb://127.0.0.1:27017/movielens.ratings.stats update=false");

            System.err.println("Example: MapReduceExercise " +
                "mongodb://127.0.0.1:27017/movielens.ratings " +
                "mongodb://127.0.0.1:27017/movielens.movies update=true");

            System.exit(-1);
        }

        Class outputValueClass = BSONWritable.class;
        Class reducerClass = Reduce.class;

        if(args[2].equals("update=true")) {
            outputValueClass = MongoUpdateWritable.class;
            reducerClass = ReduceUpdater.class;
        }

        Configuration conf = new Configuration();

        // Set MongoDB-specific configuration items

    }
}
