package pump;

import algernon.AlgyWhisperer;
import security.DatabaseCredentials;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

class Captain {

    private ExecutorService boss;
    private List<ChunkWorker> men;
    private Queue<List<PrimaryKey>> workOrders;
    private AtomicInteger workCount;
    private Integer totalWorkCount;
    private List<Future<Map<PrimaryKey, TableRow>>> pendingWorkResults;
    private String tableName;
    private DatabaseCredentials credentials;
    private List<String> pkColumnNames;
    private Header header;
    private Map<PrimaryKey,TableRow> completedWork;
    private Log logger = new Log();

    Captain(String tableName, DatabaseCredentials credentials, List<String> pkColumnNames, Header header){
        boss = Executors.newWorkStealingPool();
        men = new ArrayList<>();
        workOrders = new ConcurrentLinkedQueue<>();
        workCount = new AtomicInteger();
        pendingWorkResults  = new ArrayList<>();
        completedWork = new LinkedHashMap<>();
        totalWorkCount = 0;
        this.tableName = tableName;
        this.pkColumnNames = pkColumnNames;
        this.header = header;
        this.credentials = credentials;
    }

    private void orderWork(List<PrimaryKey> work){
        workOrders.add(work);
    }

    void orderSingleWork(PrimaryKey work){
        List<PrimaryKey> shortOrder = new ArrayList<>();
        shortOrder.add(work);
        workOrders.add(shortOrder);
    }

    void organizeWork(List<PrimaryKey> bulkWork){
        Integer q = 0;
        if(!bulkWork.isEmpty()) {
            if (bulkWork.size() > 9999) {
                for (int i = 0; i < bulkWork.size() / 10000; i++) {
                    orderWork(bulkWork.subList(q, q + 9999));
                    q = q + 10000;
                }
                orderWork(bulkWork.subList(q, bulkWork.size() - 1));
            } else {
                orderWork(bulkWork);
            }
        }
    }

    Integer getTotalWorkCount(){
        return totalWorkCount;
    }

    Map<PrimaryKey,TableRow> work() {
        totalWorkCount = workOrders.size();
        for (List<PrimaryKey> workOrder : workOrders) {
            men.add(new ChunkWorker(workCount, new AlgyWhisperer(credentials), totalWorkCount, tableName, workOrder, pkColumnNames, header));
        }

        try {
            pendingWorkResults = boss.invokeAll(men, 2, TimeUnit.HOURS);
            for (Future<Map<PrimaryKey, TableRow>> pendingWorkResult : pendingWorkResults) {
                completedWork.putAll(pendingWorkResult.get());
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        return completedWork;
    }

    Map<PrimaryKey,TableRow> workWithBatch(TableBatch batch) throws SQLException{
        totalWorkCount = workOrders.size();
        for (List<PrimaryKey> workOrder : workOrders) {
            men.add(new ChunkWorker(workCount, new AlgyWhisperer(credentials), totalWorkCount, tableName, workOrder, pkColumnNames, header));
        }
        try {
            pendingWorkResults = boss.invokeAll(men, 2, TimeUnit.HOURS);
            for (Future<Map<PrimaryKey, TableRow>> pendingWorkResult : pendingWorkResults) {
                Map<PrimaryKey,TableRow> workResult = pendingWorkResult.get();
                for(PrimaryKey pk : workResult.keySet()) {
                    TableRow newRow = workResult.get(pk);
                    completedWork.put(pk,newRow);
                    batch.addUpdateRowAndBatch(newRow);
                    workCount.getAndIncrement();
                    logger.log("retrieved remote row with PK "+newRow.getPrimaryKey().getFirstKeyValue().toString()+", "+workCount+"/"+totalWorkCount);
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return completedWork;
    }

}
