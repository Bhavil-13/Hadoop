import java.util.List;

import TripleStore.IServer;
import TripleStore.MongoDB_Server;
import TripleStore.Triple;
import TripleStore.Postgres_date_Server;


public class App {
    public static void main(String[] args)
    {

        // Instantiate postgres and mongo servers
        Postgres_date_Server postgres_new_server = new Postgres_date_Server();
        MongoDB_Server mongo_server = new MongoDB_Server();


        // Enter some data for tesing
        Boolean enterData = true;
        if (enterData)
        {
            postgres_new_server.update("ricky", "plays", "cricket");
            mongo_server.update("ricky", "plays", "badminton");
            mongo_server.update("A", "Studying", "B");
            postgres_new_server.update("A", "Studying", "C");
        }


        // merge the databases
        Boolean mergeDatabases = false;
        if (mergeDatabases) {
            mongo_server.merge(postgres_new_server);
        }

        
        // Check if the queries are working or not
        Boolean checkQuery = false;
        if (checkQuery)
        {
            checkQuery(postgres_new_server, "ricky");
            checkQuery(postgres_new_server, "A");
            checkQuery(mongo_server, "ricky");
            checkQuery(mongo_server, "A");
        }
    }



    // METHOD to run the query and print the results
    public static void checkQuery(IServer s1, String subject) {
        List<Triple> triples = s1.query(subject);
        for (Triple triple: triples) {
            System.out.println(triple.get_subject() + " " +triple.get_predicate() + " " + triple.get_object() + " " + triple.get_timestamp().toString());
        }
    }


}