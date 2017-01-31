package automation_framework;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

class Boss {
    private static Boolean working;
    private ExecutorService boss;
    private ScrubBoss scrubBoss;
	private RedXBoss redXBoss;
    private TableBoss tableBoss;
    private static AlgyWhisperer algyWhisperer;

	Boss(){
	    boss = Executors.newCachedThreadPool();
        algyWhisperer = new AlgyWhisperer(Robot.dbUsername,Robot.dbPassword,Robot.address,Robot.dbName);
        if(Robot.scrubThreadCount>0) {
            scrubBoss = new ScrubBoss();
        }
        if(Robot.redXGo) {
            redXBoss = new RedXBoss();
        }
        if(Robot.tableThreadCount>0) {
            tableBoss = new TableBoss();
        }
	}

	synchronized void startWork() throws Exception{
	    working = true;
        if(Robot.scrubThreadCount>0) {
            boss.execute(scrubBoss);
        }
        if(Robot.redXGo) {
            boss.execute(redXBoss);
        }
        if(Robot.tableThreadCount>0) {
            boss.execute(tableBoss);
        }
    }

    private class RedXBoss implements Runnable{
        ExecutorService boss;
        RedXWorker worker;

        RedXBoss(){
            boss = Executors.newSingleThreadExecutor();
            worker = new RedXWorker();
        }

        public void run(){
            do {
                Future redXCheck = boss.submit(worker);
                try {
                    redXCheck.get();
                } catch (ExecutionException | InterruptedException e) {
                    e.printStackTrace();
                }
            }while(working);
        }
    }

    private class ScrubBoss implements Runnable{
        ExecutorService boss;
        MotorPool motorPool;
        List<Future<HtmlOutputRecord>> results;

        ScrubBoss(){
            boss = Executors.newWorkStealingPool();
            motorPool = new MotorPool(Robot.scrubThreadCount);
            results = new ArrayList<>();
            ArrayList<Map<String,String>> clientRecords = algyWhisperer.getAllEligibility();
            //List<ScrubWorker> workOrder = new ArrayList<>();
            for(Map<String,String> clientRecord : clientRecords){
                //workOrder.add(new ScrubWorker(clientRecord,motorPool));
                HtmlScrubWorker worker = new HtmlScrubWorker(clientRecord,motorPool);
                Future<HtmlOutputRecord> pendingResult = boss.submit(worker);
                results.add(pendingResult);
            }
        }

        public void run(){
            Boolean doneChecking = true;
            do{
                for(Future<HtmlOutputRecord> pendingResult : results) {
                    if (!(pendingResult == null)) {
                        if(pendingResult.isDone()) {
                            try {
                                //Robot.access.finishHtmlScrubEntry(pendingResult.get());
                            } catch (Exception e) {
                                e.printStackTrace();
                                throw new RuntimeException("something went terribly wrong and I don't know what is");
                            }
                        } else {
                            doneChecking = false;
                    }
                    }
                }
            }while(!doneChecking);
        }

    }

    private class TableBoss implements Runnable{
        ExecutorService boss;
        ArrayList<TableWorker> workers;

        TableBoss(){
            boss = Executors.newFixedThreadPool(Robot.tableThreadCount);
            workers = new ArrayList<>();
            try {
                for (int q = 0; q < Robot.tableThreadCount; q++) {
                    workers.add(new TableWorker());
                }
            }catch(Exception e){
                e.printStackTrace();
            }
        }

        public void run() {
            for (TableWorker worker : workers) {
                TableManager manager = new TableManager(worker);
                boss.execute(manager);
            }
        }

        class TableManager implements Runnable{
            private TableWorker worker;

            TableManager(TableWorker worker){
                this.worker = worker;
            }

            public void run(){
                ExecutorService manager = Executors.newSingleThreadExecutor();
                do {
                    Future check = manager.submit(worker);
                    try {
                        check.get();
                    } catch (ExecutionException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }while(working);
            }
        }
    }

}