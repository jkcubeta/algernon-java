package pump;

import algernon.AlgyWhisperer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

class ChunkWorker implements Callable<Map<PrimaryKey, TableRow>>{
        private AtomicInteger chunkCount;
        private Integer tableChunkCount;
        private AlgyWhisperer whisperer;
        private String tableName;
        private List<String> pkColumnNames;
        private Header header;
        private List<PrimaryKey> keyChunk;

        ChunkWorker(AtomicInteger chunkCount, AlgyWhisperer whisperer, Integer tableChunkCount, String tableName, List<PrimaryKey> keyChunk, List<String> pkColumnNames, Header header){
            this.chunkCount = chunkCount;
            this.whisperer = whisperer;
            this.tableChunkCount = tableChunkCount;
            this.tableName = tableName;
            this.pkColumnNames = pkColumnNames;
            this.header = header;
            this.keyChunk = keyChunk;
        }

        @Override
        public Map<PrimaryKey, TableRow> call() throws Exception {
            return whisperer.getModifiedLargeTableData(tableName,header,pkColumnNames,keyChunk);
        }

}