package eligibility;

import algernon.ReplicationDatabaseConnectionError;
import pump.Log;
import security.DatabaseCredentials;
import algernon.AlgyEligibilityWhisperer;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Level;


public class Boss {
    private ExecutorService boss;
    private TableBatch batch;
    private AlgyEligibilityWhisperer whisperer;
    private MotorPool motorPool;
    private List<HtmlScrubWorker> workers;
    private Log log;

    public static void main(String[] args){
        java.util.logging.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(Level.OFF);
        java.util.logging.Logger.getLogger("com.gargoylesoftware.htmlclient").setLevel(Level.OFF);
        String secretKey = args[0];
        Integer numDrivers = Integer.parseInt(args[1]);
        Integer maxRows = Integer.parseInt(args[2]);
        Integer numRotations = Integer.parseInt(args[3]);
        DatabaseCredentials credentials = new DatabaseCredentials(secretKey);
        //if the number of rotations in set to zero, repeat the loop perpetually
        if(numRotations == 0){
            do {
                Boss boss = new Boss(numDrivers, maxRows, credentials);
                boss.startWork();
            }while(1>0);
        }else{
            for(Integer rotationCount = 0; rotationCount < numRotations; rotationCount++){
                Boss boss = new Boss(numDrivers,maxRows,credentials);
                boss.startWork();
            }
        }
    }

    private Boss(Integer numDrivers, Integer maxRows, DatabaseCredentials credentials) {
        this.boss = Executors.newWorkStealingPool();
        this.batch = new TableBatch(credentials,maxRows);
        this.whisperer = new AlgyEligibilityWhisperer(credentials);
        this.motorPool = new MotorPool(numDrivers, credentials);
        this.workers = new ArrayList<>();
        this.log = new Log();
    }

    private void startWork(){
        ArrayList<Map<String,String>> clientBatch;
        try {
            clientBatch = whisperer.getEligibilityBatch();
        }catch(SQLException e){
            e.printStackTrace();
            throw new ReplicationDatabaseConnectionError();
        }
        for(Map<String,String> client : clientBatch){
            workers.add(new HtmlScrubWorker(client,motorPool));
        }
        List<Future<HtmlOutputRecord>> results = new ArrayList<>();
        try {
            for(HtmlScrubWorker worker : workers){
                results.add(boss.submit(worker));
            }
            do {
                for (Future<HtmlOutputRecord> result : results) {
                    if (result.isDone() && !result.isCancelled()) {
                        batch.addResultAndBatch(result.get());
                        log.log("completed result id: "+result.toString());
                    }
                }
            }while(!checkWork(results));
        }catch(InterruptedException | ExecutionException e){
            e.printStackTrace();
            throw new RuntimeException("we got interrupted while working on the eligibility, consult stack trace");
        }catch(SQLException e){
            e.printStackTrace();
            throw new ReplicationDatabaseConnectionError();
        }
        try {
            batch.executeResultStatement();
        }catch(SQLException e){
            e.printStackTrace();
            throw new ReplicationDatabaseConnectionError();
        }

    }

    private boolean checkWork(List<Future<HtmlOutputRecord>> pendingResults){
        Boolean finished = true;
        for(Future<HtmlOutputRecord> pendingResult : pendingResults){
            if(!pendingResult.isDone()){
                finished = false;
            }
        }
        return finished;
    }

}
