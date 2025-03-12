package TripleStore;

import java.sql.Timestamp;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class Postgres_date_Server implements IServer{

    private Connection connection;

    public Postgres_date_Server() {
        //Initilizing the database connection and createing the triples table if it already doesn't exist
        String url = "jdbc:postgresql://localhost:5433/jdbc_testing";
        String username = "************";
        String password = "***************";

        try
        {
            connection = DriverManager.getConnection(url, username, password);
            System.out.println("Connected to the PostgreSQL server successfully.");
            // Create the table if its already not there
            createTable();
        } 
        catch (SQLException e)
        {
            System.out.println("Connection failed. Check output console.");
            e.printStackTrace();
        } 
        catch (Exception e)
        {
            System.out.println("An error occurred while creating or deleteing the table.");
            e.printStackTrace();
        }
    }



    // Kept public for now while developing. Figure out how to ensure the closing of connection automatically
    public void closeConnection() {
        try {
            connection.close();
            System.out.println("Connection closed successfully.");
        } catch (SQLException e) {
            System.out.println("Error occurred while closing the connection.");
            e.printStackTrace();
        }
    }




    // Method to create a new table
    public void createTable() {
        try (Statement statement = connection.createStatement()) {
            String createTableSQL = "CREATE TABLE IF NOT EXISTS triples (" +
                                    "subject VARCHAR(50) NOT NULL," +
                                    "predicate VARCHAR(50) NOT NULL," +
                                    "object VARCHAR(50) NOT NULL," +
                                    "timeStamp TIMESTAMP NOT NULL,"+
                                    "PRIMARY KEY (subject, predicate)" +
                                    ")";

            

            statement.executeUpdate(createTableSQL);
            System.out.println("Table 'triples' created successfully.");

            // Create indexes
            // For facilating the query method
            String createIndexSubjectSQL = "CREATE INDEX IF NOT EXISTS idx_subject ON triples (subject)";
            statement.executeUpdate(createIndexSubjectSQL);
            System.out.println("Index on 'subject' created successfully.");
            
            // For facilating the update and merge method
            String createIndexSubjectPredicateSQL = "CREATE INDEX IF NOT EXISTS idx_subject_predicate ON triples (subject, predicate)";
            statement.executeUpdate(createIndexSubjectPredicateSQL);
            System.out.println("Composite index on 'subject' and 'predicate' created successfully.");
            
            // For facilating the merge method
            String createIndexSubjectPredicateTimestampSQL = "CREATE INDEX IF NOT EXISTS idx_subject_predicate_timestamp ON triples (subject, predicate, timeStamp)";
            statement.executeUpdate(createIndexSubjectPredicateTimestampSQL);
            System.out.println("Composite index on 'subject', 'predicate', and 'timeStamp' created successfully.");


        } catch (SQLException e) {
            System.out.println("SQL error occurred while creating the triples table.");
            e.printStackTrace();
        }
    }




    @Override
    public List<Triple> query(String subject) {
        List<Triple> triples = new ArrayList<>();
        String query = "SELECT * FROM triples WHERE subject = ?";
        
        try (PreparedStatement statement = connection.prepareStatement(query)) {

            statement.setString(1, subject);
            ResultSet resultSet = statement.executeQuery();
            
            while (resultSet.next()) {
                Timestamp time = resultSet.getTimestamp("timeStamp");
                LocalDateTime timeStamp = time.toLocalDateTime();
                String predicate = resultSet.getString("predicate");
                String object = resultSet.getString("object");
                Triple triple = new Triple(subject, predicate, object, timeStamp);
                triples.add(triple);
            }

        } catch (SQLException e) {
            System.out.println("SQL error occurred while querying triples by subject.");
            e.printStackTrace();
        }
        return triples;
    }




    @Override
    public List<Triple> get_all() {
        List<Triple> triples = new ArrayList<>();
        String query = "SELECT * FROM triples";
        
        try (PreparedStatement statement = connection.prepareStatement(query)) {

            ResultSet resultSet = statement.executeQuery();
            
            while (resultSet.next()) {
                Timestamp time = resultSet.getTimestamp("timeStamp");
                LocalDateTime timeStamp = time.toLocalDateTime();
                String subject = resultSet.getString("subject");
                String predicate = resultSet.getString("predicate");
                String object = resultSet.getString("object");
                Triple triple = new Triple(subject, predicate, object, timeStamp);
                // System.out.println(subject + " " + predicate + " " + object);
                triples.add(triple);
            }

        } catch (SQLException e) {
            System.out.println("SQL error occurred while querying triples by subject.");
            e.printStackTrace();
        }
        return triples;
    }





    @Override
    public void update(String subject, String predicate, String object) {
        try {
            String updateQuery = "UPDATE triples SET object = ?, timeStamp = ? WHERE subject = ? AND predicate = ?";
            
            try (PreparedStatement updateStatement = connection.prepareStatement(updateQuery)) {
                LocalDateTime time = LocalDateTime.now();
                Timestamp timestamp = Timestamp.valueOf(time);
                updateStatement.setString(1, object);
                updateStatement.setTimestamp(2, timestamp);
                updateStatement.setString(3, subject);
                updateStatement.setString(4, predicate);
                int rowsUpdated = updateStatement.executeUpdate();
                
                if (rowsUpdated == 0) {
                    System.out.println("No rows were found during update and so now inserting a new row instead.");
                    String insertQuery = "INSERT INTO triples (subject, predicate, object, timeStamp) VALUES (?, ?, ?, ?)";
                    try (PreparedStatement insertStatement = connection.prepareStatement(insertQuery)) {
                        insertStatement.setString(1, subject);
                        insertStatement.setString(2, predicate);
                        insertStatement.setString(3, object);
                        insertStatement.setTimestamp(4, timestamp);
                        insertStatement.executeUpdate();
                    }
                } else {
                    System.out.println("Row updated successfully.");
                }
            }
        } catch (SQLException e) {
            System.out.println("SQL error occurred while updating the entry.");
            e.printStackTrace();
        }
    }





    @Override
    public void push(String subject, String predicate, String object, String timeStamp) {
        try 
        {
            LocalDateTime time = LocalDateTime.parse(timeStamp);
            Timestamp t = Timestamp.valueOf(time);
            String selectQuery = "SELECT * FROM triples where subject = ? AND predicate = ?";

            try (PreparedStatement selectStatement = connection.prepareStatement(selectQuery))
            {
                selectStatement.setString(1, subject);
                selectStatement.setString(2, predicate);
                try(ResultSet resultSet = selectStatement.executeQuery()) 
                {
                    // If no match than just add a new entry
                    if (!resultSet.next())
                    {
                        String insertQuery = "INSERT INTO triples (subject, predicate, object, timeStamp) VALUES (?, ?, ?, ?)";
                        try (PreparedStatement insertStatement = connection.prepareStatement(insertQuery)) 
                        {
                            insertStatement.setString(1, subject);
                            insertStatement.setString(2, predicate);
                            insertStatement.setString(3, object);
                            insertStatement.setTimestamp(4, t);
                            insertStatement.executeUpdate();
                        }
                    }

                    // If match found than compare the timestamp values for figuring the priority
                    else
                    {
                        Timestamp t_to_check = resultSet.getTimestamp("timeStamp");
                        if (t_to_check.before(t))
                        {
                            String updateQuery = "UPDATE triples SET object = ?, timeStamp = ? WHERE subject = ? AND predicate = ?";
                            try (PreparedStatement updateStatement = connection.prepareStatement(updateQuery)) 
                            {
                                updateStatement.setString(1, object);
                                updateStatement.setTimestamp(2, t);
                                updateStatement.setString(3, subject);
                                updateStatement.setString(4, predicate);
                                updateStatement.executeUpdate();
                            }
                        }
                    }


                }
            }
        } 
        catch (SQLException e) 
        {
            System.out.println("SQL error occurred while updating the entry.");
            e.printStackTrace();
        }
    }



    @Override
    public void merge(IServer _destination) {
        long t1 = System.nanoTime();

        // This should make all the requierd changes in the destination server
        this.iterate_all(_destination);

        long t2 = System.nanoTime();

        // This should make all the required changes in the souce server
        _destination.iterate_all(this);

        long t3 = System.nanoTime();

        System.out.println("Destination server changes took: " + (t2-t1) + " nanoseconds.");
        System.out.println("Source server changes took: " + (t3-t2) + " nanoseconds.");
        System.out.println("Total time taken: " + (t3-t1) + " nanoseconds.");

    }

    

    @Override
    public void iterate_all(IServer destinationServer)
    {
        // Get the triples list in batches
        int batchSize = 10;
        int offset = 0;
        String query = "SELECT * FROM triples LIMIT ? OFFSET ?";
        try (PreparedStatement statement = connection.prepareStatement(query))
        {
            statement.setInt(1, batchSize);

            while(true) 
            {
                statement.setInt(2, offset);
                ResultSet resultSet = statement.executeQuery();

                // Process the current batch of rows
                int rowCount = 0;
                while (resultSet.next()) {
                    String t = resultSet.getTimestamp("timeStamp").toLocalDateTime().toString();
                    String subject = resultSet.getString("subject");
                    String predicate = resultSet.getString("predicate");
                    String object = resultSet.getString("object");
                    destinationServer.push(subject, predicate, object, t);
                    rowCount++;
                }

                // Check if there are more rows to fetch
                if (rowCount >= batchSize)  {
                    offset += batchSize; // Move to the next batch
                } 
                else {
                    break; // No more rows to fetch
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}