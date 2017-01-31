package pump;

import security.DatabaseCredentials;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Main {

    public static void main(String[] args){
        Integer numThreads;
        Integer numLoops;
        String password;
        try{
            numLoops = Integer.parseInt(args[0]);
            numThreads = Integer.parseInt(args[1]);
            password = args[2];
        }catch(Exception e){
            System.out.println("invalid parameter set for arguments, closing");
            System.exit(0);
            return;
        }
        System.out.println("welcome to algernon, currently running a remote pump operation with "+numThreads+" threads");
        DatabaseCredentials credentials = new DatabaseCredentials(password);
        ExecutorService boss = Executors.newFixedThreadPool(numThreads);
        List<TableWorker> workers = new ArrayList<>();
        List<Future> checks = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            workers.add(new TableWorker(credentials));
        }
        if(numLoops == 0){
            Boolean working = true;
            do {
                for (TableWorker worker : workers) {
                    checks.add(boss.submit(worker));
                }
                for (Future check : checks) {
                    try {
                        check.get();
                    } catch (ExecutionException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }while(working);
        }else{
            for (int i = 0; i < numLoops; i++) {
                for (TableWorker worker : workers) {
                    checks.add(boss.submit(worker));
                }
                for (Future check : checks) {
                    try {
                        check.get();
                    } catch (ExecutionException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }


    }
}
