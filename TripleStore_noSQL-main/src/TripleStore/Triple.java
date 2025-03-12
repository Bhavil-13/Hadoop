package TripleStore;

import java.time.LocalDateTime;
import java.util.Objects;

public class Triple {

    // attributes
    private String subject;
    private String predicate;
    private String object;
    private LocalDateTime timestamp;

    public Triple() {
        subject = new String();
        predicate = new String();
        object = new String();
    }

    public Triple(String _subject, String _predicate, String _object, LocalDateTime _timestamp) {
        subject = _subject;
        predicate = _predicate;
        object = _object;
        timestamp = _timestamp;
    }

    public String get_subject() {
        return subject;
    }

    public String get_predicate() {
        return predicate;
    }

    public String get_object() {
        return object;
    }

    public LocalDateTime get_timestamp() {
        return timestamp;
    }

    public void set_subject(String _subject) {
        subject = _subject;
    }

    public void set_predicate(String _predicate) {
        predicate = _predicate;
    }

    public void set_object(String _object) {
        object = _object;
    }

    public void set_timestamp(LocalDateTime _timestamp) {
        timestamp = _timestamp;
    }

    public Boolean equals(Triple _other) {
        return Objects.equals(subject, _other.subject) &&
                Objects.equals(predicate, _other.predicate) &&
                Objects.equals(object, _other.object) &&
                Objects.equals(timestamp, _other.timestamp);
    }

}
