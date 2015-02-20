package com.mongodb.workshop;

import com.mongodb.AggregationOptions;
import com.mongodb.AggregationOptions.OutputMode;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.List;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.slf4j.LoggerFactory.getLogger;

public class DataSet {
    private static final Logger LOG = getLogger(DataSet.class);
    public static final String MOVIEID = "movieid";
    public static final String RATINGS = "ratings";
    public static final String GENRES = "genres";
    public static final String RATING = "rating";
    public static final String USERID = "userid";
    public static final String MOVIES = "movies";
    public static final String USERS = "users";
    public static final String $_MOVIEID = "$" + MOVIEID;
    public static final String $_RATING = "$" + RATING;
    public static final String $_USERID = "$" + USERID;
    public static final String TS = "ts";
    public static final String TITLE = "title";
    public static final String YEAR = "year";
    public static final String ID = "_id";

    public static void main(final String[] args) throws IOException {
        final MongoClient client = new MongoClient();

        final DB db = client.getDB("movielens");
        db.dropDatabase();
        importMovies(args[0], db);
        importRatings(args[1], db);
        aggregateUsers(db);
        aggregateMovies(db);
        createGenres(db);
        ensureIndexes(client.getDB("movielens"));
    }

    private static BasicDBObject o(final String key, final Object value) {
        return new BasicDBObject(key, value);
    }

    private static void createGenres(final DB db) {
        LOG.info("Creating genres");
        final DBCollection movies = db.getCollection(MOVIES);
        final DBCollection genres = db.getCollection(GENRES);
        genres.remove(new BasicDBObject());
        final Cursor aggregate = movies.aggregate(asList(o("$unwind", "$genres"),
                                                         o("$group", o("_id", "$genres")
                                                                         .append("count", o("$sum", 1)))),
                                                  AggregationOptions.builder()
                                                                    .outputMode(OutputMode.CURSOR)
                                                                    .build());
        int count = 0;
        while (aggregate.hasNext()) {
            final DBObject next = aggregate.next();
            genres.insert(o("_id", count++)
                              .append("name", next.get("_id"))
                              .append("count", next.get("count")));
        }
        LOG.info("Done creating genres");
    }

    private static void ensureIndexes(final DB db) {
        db.getCollection(MOVIES).createIndex(o(MOVIEID, 1));
        db.getCollection(MOVIES).createIndex(o(RATINGS, 1));
        db.getCollection(MOVIES).createIndex(o(GENRES, 1));
        db.getCollection(RATINGS).createIndex(o(USERID, 1).append(MOVIEID, 1));
        db.getCollection(USERS).createIndex(o(USERID, 1));
        db.getCollection(GENRES).createIndex(o("name", 1));
    }

    private static void aggregateMovies(final DB db) {
        LOG.info("Aggregating movie data");
        final DBCollection ratings = db.getCollection(RATINGS);
        final DBObject groupBy = o("$group",
                                   o(ID, $_MOVIEID)
                                       .append(RATINGS, o("$sum", 1))
                                       .append("total_rating", o("$sum", $_RATING)));
        final List<DBObject> pipeline = asList(groupBy);
        final Cursor aggregate = ratings.aggregate(pipeline, AggregationOptions
                                                                 .builder()
                                                                 .outputMode(OutputMode.CURSOR)
                                                                 .build());

        final DBCollection movies = db.getCollection(MOVIES);
        while (aggregate.hasNext()) {
            final BasicDBObject next = (BasicDBObject) aggregate.next();
            final BasicDBObject query = o(MOVIEID, next.get(ID));
            next.remove(ID);
            movies.update(query, o("$set", next), false, false);
        }

        LOG.info("Done aggregating movie data");
    }

    private static void aggregateUsers(final DB db) {
        LOG.info("Aggregating user data");
        final DBCollection ratings = db.getCollection(RATINGS);
        final DBObject groupBy = o("$group",
                                   o(ID, $_USERID)
                                       .append(RATINGS, o("$sum", 1)));
        final DBObject out = o("$out", USERS);
        final List<DBObject> pipeline = asList(groupBy, out);
        ratings.aggregate(pipeline);

        LOG.info("Done aggregating user data");
    }

    private static void importRatings(final String path, final DB db) throws IOException {
        LOG.info("Importing ratings");

        int count = 0;
        final DBCollection ratings = db.getCollection(RATINGS);
        ratings.drop();

        BulkWriteOperation ratingUpdate = ratings.initializeUnorderedBulkOperation();

        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = reader.readLine()) != null) {
                final String[] split = line.split("::");
                final Integer movieId = Integer.valueOf(split[0]);
                final Double rating = Double.valueOf(split[2]);
                final Integer userId = Integer.valueOf(split[1]);
                ratingUpdate.insert(o(MOVIEID, movieId)
                                        .append(USERID, userId)
                                        .append(RATING, rating)
                                        .append(TS, new Date(Long.valueOf(split[3]))));
                count++;

                if (count % 1000 == 0) {
                    if (count % 100_000 == 0) {
                        LOG.info(format("Writing batch to ratings (%d)", count));
                    }
                    ratingUpdate.execute();
                    ratingUpdate = ratings.initializeUnorderedBulkOperation();
                }
            }
            ratingUpdate.execute();
        }
        LOG.info("Done importing ratings");
    }

    private static void importMovies(final String path, final DB db) throws IOException {
        LOG.info("Importing movies");
        final DBCollection coll = db.getCollection(MOVIES);
        coll.drop();
        BulkWriteOperation bulk = coll.initializeUnorderedBulkOperation();
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            String line;
            int count = 0;
            while ((line = reader.readLine()) != null) {
                final String[] split = line.split("::");
                String title = split[1];
                Integer year = null;
                if (title.endsWith(")")) {
                    title = title.substring(0, title.lastIndexOf('(')).trim();
                    year = Integer.valueOf(split[1].substring(title.length() + 2, split[1].length() - 1));
                }
                final BasicDBObject movie = o(MOVIEID, Integer.valueOf(split[0]));
                movie.append(TITLE, title);
                if (year != null) {
                    movie.append(YEAR, year);
                }
                movie.append(GENRES, split[2].split("\\|"));
                bulk.insert(movie);
                count++;

                if (count % 1000 == 0) {
                    bulk.execute();
                    bulk = coll.initializeUnorderedBulkOperation();
                }
            }
            bulk.execute();
        }
        LOG.info("Done importing movies");
    }
}
