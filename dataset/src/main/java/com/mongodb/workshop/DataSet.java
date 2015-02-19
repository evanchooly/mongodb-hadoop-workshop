package com.mongodb.workshop;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;

public class DataSet {
    public static void main(final String[] args) throws IOException {
        final MongoClient client = new MongoClient();

        //        fmovies = open(sys.argv[1])
        //        fratings = open(sys.argv[2])

        final DB db = client.getDB("movielens");
        importMovies(args[0], db);
        importRatings(args[1], db);
        //        create_genres(client);
        //        ensure_indexes(client);

    }

    private static void importRatings(final String path, final DB db) throws IOException {
        System.out.println("Importing ratings");

        int count = 0;
        final DBCollection ratings = db.getCollection("ratings");
        final DBCollection users = db.getCollection("users");
        final DBCollection movies = db.getCollection("movies");

        ratings.drop();
        users.drop();

        BulkWriteOperation ratingUpdate = ratings.initializeUnorderedBulkOperation();
        BulkWriteOperation movieUpdate = movies.initializeUnorderedBulkOperation();
        BulkWriteOperation userUpdate = users.initializeUnorderedBulkOperation();

        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = reader.readLine()) != null) {
                final String[] split = line.split("::");
                final Integer movieId = Integer.valueOf(split[0]);
                final Double rating = Double.valueOf(split[2]);
                final Integer userId = Integer.valueOf(split[1]);
                ratingUpdate.insert(new BasicDBObject("movieid", movieId)
                                        .append("userid", userId)
                                        .append("rating", rating)
                                        .append("ts", new Date(Long.valueOf(split[3]))));
                movieUpdate
                    .find(new BasicDBObject("movieid", movieId))
                    .update(new BasicDBObject("$inc",
                                              new BasicDBObject("ratings", 1)
                                                  .append("total_rating", rating)));

                userUpdate
                    .find(new BasicDBObject("userid", userId))
                    .upsert()
                    .update(new BasicDBObject("$inc", new BasicDBObject("ratings", 1)));
                count++;

                if (count % 1000 == 0) {
                    System.out.printf("Writing batch to ratings (%d)\n", count);
                    ratingUpdate.execute();
                    movieUpdate.execute();
                    userUpdate.execute();

                    ratingUpdate = ratings.initializeUnorderedBulkOperation();
                    movieUpdate = movies.initializeUnorderedBulkOperation();
                    userUpdate = users.initializeUnorderedBulkOperation();
                }
            }
        }
        System.out.println("Done importing ratings");
    }

    private static void importMovies(final String path, final DB db) throws IOException {
        System.out.println("Importing movies");
        final DBCollection coll = db.getCollection("movies");
        coll.drop();
        BulkWriteOperation bulk = null;
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            String line;
            int count = 0;
            while ((line = reader.readLine()) != null) {
                final String[] split = line.split("::");
                if (count % 1000 == 0) {
                    bulk = coll.initializeUnorderedBulkOperation();
                }
                String title = split[1];
                Integer year = null;
                if (title.endsWith(")")) {
                    title = title.substring(0, title.lastIndexOf('(')).trim();
                    year = Integer.valueOf(split[1].substring(title.length() + 2, split[1].length() - 1));
                }
                final BasicDBObject movie = new BasicDBObject("movieid", Integer.valueOf(split[0]));
                movie.append("title", title);
                if (year != null) {
                    movie.append("year", year);
                }
                movie.append("genres", split[2].split("\\|"));
                bulk.insert(movie);
                count++;

                if (count % 1000 == 0) {
                    System.out.println("Writing batch to movies collection");
                    bulk.execute();
                }
            }
            bulk.execute();
        }
        System.out.println("Done importing movies");
    }
}
