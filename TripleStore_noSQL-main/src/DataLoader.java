import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

// import java.util.List;

// import TripleStore.IServer;
// import TripleStore.MongoDB_Server;
// import TripleStore.Triple;
import TripleStore.Postgres_date_Server;
import TripleStore.MongoDB_Server;

public class DataLoader {
    
    // Assume 'update' method is defined in a class named TripleUpdater
    
    public static void main(String[] args) {

        long t1 = System.nanoTime();

        MongoDB_Server mongo_server = new MongoDB_Server();
        // Postgres_date_Server postgres_server = new Postgres_date_Server(); 
        
        String filePath = "/home/ricky/Desktop/sem6/NoSql/TripleStore_project/yago_full_clean.tsv"; // Path to your dataset.tsv file

        int count = 0;
        
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                // Split the line by tab character assuming dataset.tsv is tab-separated
                String[] parts = line.split(" ");
                if (parts.length == 3) {
                    // Extract subject, predicate, and object from the line
                    String subject = parts[0];
                    String predicate = parts[1];
                    String object = parts[2];
                    
                    // Call the update method with extracted values
                    mongo_server.update(subject, predicate, object);
                    count += 1;
                    System.out.println(count);
                    if (count == 1192368) {
                        break;
                    }
                } else {
                    System.out.println("Invalid line format: " + line);
                }
            }
            long t2 = System.nanoTime();
            System.out.println("Total time taken: " + (t2-t1) + " nanoseconds.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
