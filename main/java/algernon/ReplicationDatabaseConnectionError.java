package algernon;

public class ReplicationDatabaseConnectionError extends RuntimeException {

    public ReplicationDatabaseConnectionError(){
        super("can't connect to replication database, check configuration and try again");
    }
}
