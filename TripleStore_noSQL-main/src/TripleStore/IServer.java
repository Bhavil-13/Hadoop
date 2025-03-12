package TripleStore;

import java.util.List;

public interface IServer {
    List<Triple> query(String _subject);
    // returns all tuples with a given subject

    List<Triple> get_all();
    // returns all the tuples

    void update(String _subject, String _predicate, String _object);
    // insert tuple if not found, else updates the tuple

    void push(String _subject, String _predicate, String _object, String _timestamp);
    // merge update, update while merging
    // renamed merge_update as push

    void merge(IServer _destination);
    // merging with other server

    void iterate_all(IServer _destination);
    // used in the merge function
}