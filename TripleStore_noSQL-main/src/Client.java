import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import TripleStore.IServer;
import TripleStore.MongoDB_Server;
import TripleStore.Postgres_date_Server;
import TripleStore.Triple;

public class Client {
    public static void main(String[] args) 
    {
        // Instantiate postgres and mongo servers
        Postgres_date_Server postgres_new_server = new Postgres_date_Server();
        MongoDB_Server mongo_server = new MongoDB_Server();

        // Create the list of servers. The order is important for now.
        List<IServer> serverList = Arrays.asList(postgres_new_server, mongo_server);

        Scanner scanner = new Scanner(System.in);
        mainMenu(scanner, serverList);
        scanner.close();
    }




    // Gets a server option from the user
    public static int handleServerSelection(Scanner scanner, List<IServer> serverList)
    {
        System.out.println("");
        System.out.println("Select a server:");
        System.out.println("1. postgres server");
        System.out.println("2. mongo server");
        // for(int option = 1; option<=serverList.size(); option++) {
        //     System.out.println(option + ". server" + option);
        // }

        int serverOption = scanner.nextInt();
        scanner.nextLine(); // Consume newline

        if ((serverOption > serverList.size()) || (serverOption < 1))
        {
            System.out.println("Invalid option.");
            serverOption = handleServerSelection(scanner, serverList);
        }
        return serverOption;
    }



    // No exit option given to the client for now
    public static void mainMenu(Scanner scanner, List<IServer> serverList)
    {
        //Ask the user to select a server.
        int serverOption = handleServerSelection(scanner, serverList);
        IServer server = serverList.get(serverOption - 1);

        // Now handle the main 3 opttions for this server
        handleMainOptions(scanner, serverList, server);
        return;
    }


    public static void handleMainOptions(Scanner scanner,List<IServer> serverList, IServer server)
    {
        // Print main options
        System.out.println("");
        System.out.println("Select an option:");
        System.out.println("1. Query");
        System.out.println("2. Update");
        System.out.println("3. Merge");
        System.out.println("4. Back to server selection");

        // Get user input
        int mainOption = scanner.nextInt();
        scanner.nextLine(); // Consume newline

        if (mainOption == 1) {
            handleQueryOption(scanner, serverList, server);
            return;
        } 
        else if (mainOption == 2) {
            handleUpdateOption(scanner, serverList, server);
            return;
        } 
        else if (mainOption == 3) {
            handleMergeOption(scanner, serverList, server);
            return;
        } 
        else if (mainOption == 4) {
            mainMenu(scanner, serverList);
            return;
        } 
        else {
            System.out.println("Invalid option.Please select from one of the given options");
            handleMainOptions(scanner, serverList, server);
            return;
        }       
    }



    private static void handleMergeOption(Scanner scanner, List<IServer> serverList, IServer server) {
        System.out.println("");
        System.out.println("Provide the destination server to merge with");
        int serverOption = handleServerSelection(scanner, serverList);
        IServer destinatinoServer = serverList.get(serverOption - 1);
        server.merge(destinatinoServer);
        handleMainOptions(scanner, serverList, server);
        return;
    }




    private static void handleUpdateOption(Scanner scanner, List<IServer> serverList, IServer server) 
    {
        System.out.println("");
        System.out.println("Provide the subject, predicate and object for update");
        System.out.print("Subject : ");
        String subject = scanner.nextLine();
        System.out.print("Predicate : ");
        String predicate = scanner.nextLine();
        System.out.print("Object : ");
        String object = scanner.nextLine();
        
        server.update(subject, predicate, object);
        handleMainOptions(scanner, serverList, server);
        return;
    }




    private static void handleQueryOption(Scanner scanner, List<IServer> serverList, IServer server)
    {
        System.out.println("");
        System.out.println("Enter the subject:");
        String subject = scanner.nextLine();
        List<Triple> triples = server.query(subject);
        for (Triple triple : triples) {
            System.out.println();
            System.out.println("Subject: " + triple.get_subject());
            System.out.println("Predicate: " + triple.get_predicate());
            System.out.println("Object: " + triple.get_object());
            System.out.println("Timestamp: " + triple.get_timestamp().toString());
            System.out.println();
        }
        handleMainOptions(scanner, serverList, server);
        return;
    }


}






