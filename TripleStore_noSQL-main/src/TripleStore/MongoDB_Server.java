package TripleStore;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MongoDB_Server implements IServer {

    private MongoClient mongoClient;
    private MongoCollection<Document> collection = null;
    private MongoDatabase database;

    public MongoDB_Server() {

        String uri = "mongodb://localhost:27017/";
        try {
            mongoClient = MongoClients.create(uri);
            database = mongoClient.getDatabase("TripleStore");
            collection = database.getCollection("Triples");

            // INDEXING
            String resultCreateIndex = collection.createIndex(Indexes.ascending("subject", "predicate"));
            System.out.println(String.format("Index created: %s", resultCreateIndex));

        } finally {

            if (collection == null)
                System.out.println("collection is null");
            else
                System.out.println("Connection established");
        }
    }

    @Override
    public List<Triple> query(String _subject) {
        // create projection fields
        Bson projectionFields = Projections.fields(
                Projections.excludeId());

        // retrieve the tuples
        MongoCursor<Document> cursor = collection.find(eq("subject", _subject))
                .projection(projectionFields)
                .iterator();

        // create the final lis
        List<Triple> res_lis = new ArrayList<>();
        try {
            while (cursor.hasNext()) {

                // parsing json and making triple
                ObjectMapper objectMapper = new ObjectMapper();
                String json_str = cursor.next().toJson();

                try {
                    JsonNode jsonNode = objectMapper.readTree(json_str);

                    Triple tri = new Triple(jsonNode.get("subject").asText(), jsonNode.get("predicate").asText(),
                            jsonNode.get("object").asText(), LocalDateTime.parse(jsonNode.get("timestamp").asText()));

                    res_lis.add(tri);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } finally {
            cursor.close();
        }

        return res_lis;
    }

    @Override
    public List<Triple> get_all() {

        // create projection fields
        Bson projectionFields = Projections.fields(
                Projections.excludeId());

        // retrieve the tuples
        MongoCursor<Document> cursor = collection.find()
                .projection(projectionFields)
                .iterator();

        // create the final lis
        List<Triple> res_lis = new ArrayList<>();
        try {
            while (cursor.hasNext()) {

                // parsing json and making triple
                ObjectMapper objectMapper = new ObjectMapper();
                String json_str = cursor.next().toJson();

                try {
                    JsonNode jsonNode = objectMapper.readTree(json_str);

                    Triple tri = new Triple(jsonNode.get("subject").asText(), jsonNode.get("predicate").asText(),
                            jsonNode.get("object").asText(), LocalDateTime.parse(jsonNode.get("timestamp").asText()));

                    res_lis.add(tri);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } finally {
            cursor.close();
        }

        return res_lis;
    }

    @Override
    public void update(String _subject, String _predicate, String _object) {

        // create projection field
        Bson projectionFields = Projections.fields(
                Projections.include("subject", "predicate", "object", "timestamp"),
                Projections.excludeId());

        // System.out.println("HELLOOOOO");
        // retrieve the record
        Document doc = collection.find(and(eq("subject", _subject), eq("predicate", _predicate)))
                .projection(projectionFields)
                .first();

        // System.out.println("HELLO");
        if (doc == null) {
            // record not found, insert one
            try {
                // Inserts a sample document describing a movie into the collection
                InsertOneResult result = collection.insertOne(new Document()
                        .append("_id", new ObjectId())
                        .append("subject", _subject)
                        .append("predicate", _predicate)
                        .append("object", _object)
                        .append("timestamp", LocalDateTime.now().toString()));
                // Prints the ID of the inserted document
                System.out.println("Success! Inserted document id: " + result.getInsertedId());

                // Prints a message if any exceptions occur during the operation
            } catch (MongoException me) {
                System.err.println("Unable to insert due to an error: " + me);
            }
        }

        else {
            // create query
            Document query = new Document().append("subject", _subject).append("predicate", _predicate);
            // create updates
            Bson updates = Updates.combine(
                    Updates.set("object", _object),
                    Updates.set("timestamp", LocalDateTime.now().toString()));

            UpdateOptions options = new UpdateOptions().upsert(true);
            try {
                // Updates the first document that has a "title" value of "Cool Runnings 2"
                UpdateResult result = collection.updateOne(query, updates, options);
                // Prints the number of updated documents and the upserted document ID, if an
                // upsert was performed
                System.out.println("Modified document count: " + result.getModifiedCount());
                System.out.println("Upserted id: " + result.getUpsertedId());

                // Prints a message if any exceptions occur during the operation
            } catch (MongoException me) {
                System.err.println("Unable to update due to an error: " + me);
            }
        }

    }




//     public void merge(IServer _destination) {
//     long t1 = System.nanoTime();
    
//     List<Triple> triplesFromSource = this.get_all();
//     List<Triple> triplesFromDestination = _destination.get_all();
    
//     List<Triple> agreedTriples = new ArrayList<>();

//     // Run Bracha’s consensus for each triple
//     for (Triple triple : triplesFromSource) {
//         if (brachaAgreement(triple, _destination)) {
//             agreedTriples.add(triple);
//         }
//     }

//     for (Triple triple : triplesFromDestination) {
//         if (brachaAgreement(triple, this)) {
//             agreedTriples.add(triple);
//         }
//     }

//     // Apply the agreed triples
//     for (Triple triple : agreedTriples) {
//         this.push(triple.get_subject(), triple.get_predicate(), triple.get_object(), triple.get_timestamp().toString());
//         _destination.push(triple.get_subject(), triple.get_predicate(), triple.get_object(), triple.get_timestamp().toString());
//     }

//     long t3 = System.nanoTime();
//     System.out.println("Total time taken with consensus: " + (t3-t1) + " nanoseconds.");
// }


    

    @Override
    public void merge(IServer _destination) {

        long t1 = System.nanoTime();
        this.iterate_all(_destination);
        long t2 = System.nanoTime();
        _destination.iterate_all(this);
        long t3 = System.nanoTime();
        System.out.println("Destination server changes took: " + (t2-t1) + " nanoseconds.");
        System.out.println("Source server changes took: " + (t3-t2) + " nanoseconds.");
        System.out.println("Total time taken: " + (t3-t1) + " nanoseconds.");
    }

    @Override
    public void push(String _subject, String _predicate, String _object, String _timestamp) {
        // create projection field
        Bson projectionFields = Projections.fields(
                Projections.include("subject", "predicate", "object", "timestamp"),
                Projections.excludeId());

        // retrieve the record
        Document doc = collection.find(and(eq("subject", _subject), eq("predicate", _predicate)))
                .projection(projectionFields)
                .first();

        if (doc == null) {
            // record not found, insert one
            try {
                // Inserts a sample document describing a movie into the collection
                InsertOneResult result = collection.insertOne(new Document()
                        .append("_id", new ObjectId())
                        .append("subject", _subject)
                        .append("predicate", _predicate)
                        .append("object", _object)
                        .append("timestamp", _timestamp));
                // Prints the ID of the inserted document
                System.out.println("Success! Inserted document id: " + result.getInsertedId());

                // Prints a message if any exceptions occur during the operation
            } catch (MongoException me) {
                System.err.println("Unable to insert due to an error: " + me);
            }
        }

        else {
            // check that the previous time was older
            if (LocalDateTime.parse(_timestamp).isAfter(LocalDateTime.parse((String) doc.get("timestamp"))) == false)
                return;

            // create query
            Document query = new Document().append("subject", _subject).append("predicate", _predicate);
            // create updates
            Bson updates = Updates.combine(
                    Updates.set("object", _object),
                    Updates.set("timestamp", _timestamp));

            UpdateOptions options = new UpdateOptions().upsert(true);
            try {
                // Updates the first document that has a "title" value of "Cool Runnings 2"
                UpdateResult result = collection.updateOne(query, updates, options);
                // Prints the number of updated documents and the upserted document ID, if an
                // upsert was performed
                System.out.println("Modified document count: " + result.getModifiedCount());
                System.out.println("Upserted id: " + result.getUpsertedId());

                // Prints a message if any exceptions occur during the operation
            } catch (MongoException me) {
                System.err.println("Unable to update due to an error: " + me);
            }
        }
    }

    @Override
    public void iterate_all(IServer _destination) {
        // create projection field
        Bson projectionFields = Projections.fields(
                Projections.include("subject", "predicate", "object", "timestamp"),
                Projections.excludeId());

        int record_count = (int) collection.countDocuments();
        int page_size = 1000;
        int offset = 0;
        int current_count = 0;

        while (true) {
            // retrieve the tuples
            MongoCursor<Document> cursor = collection.find().skip(offset).limit(page_size)
                    .projection(projectionFields)
                    .iterator();

            offset += page_size;
            try {
                while (cursor.hasNext()) {
                    // System.out.println("INSIDE ITERATE_ALL CURSOR LOOP");
                    // parsing json and making triple
                    ObjectMapper objectMapper = new ObjectMapper();
                    String json_str = cursor.next().toJson();

                    try {
                        JsonNode jsonNode = objectMapper.readTree(json_str);

                        Triple tri = new Triple(jsonNode.get("subject").asText(), jsonNode.get("predicate").asText(),
                                jsonNode.get("object").asText(),
                                LocalDateTime.parse(jsonNode.get("timestamp").asText()));

                        // DO WHATE EVER HERE
                        _destination.push(tri.get_subject(), tri.get_predicate(), tri.get_object(),
                                tri.get_timestamp().toString());

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } finally {
                cursor.close();
            }
            current_count += page_size;

            if (current_count > record_count)
                break;
        }

    }
}
