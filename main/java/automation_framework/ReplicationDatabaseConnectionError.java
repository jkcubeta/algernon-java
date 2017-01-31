package automation_framework;

class ReplicationDatabaseConnectionError extends RuntimeException {

    ReplicationDatabaseConnectionError(){
        super("can't connect to replication database, check configuration and try again");
    }
}
