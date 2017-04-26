package system.scidb;

import benchmark.BenchmarkContext;
import benchmark.QueryExecutor;
import benchmark.operations.OperationsBenchmarkDataManager;
import util.DomainUtil;

import java.text.MessageFormat;
import java.util.List;

/**
 * Created by danut on 25.04.17.
 */
public class SciDBOperationsBenchmarkDataManager extends OperationsBenchmarkDataManager<SciDBSystem> {

    private static final int TYPE_SIZE = 8;
    private static final String TYPE_BASE = "double";

    public SciDBOperationsBenchmarkDataManager(SciDBSystem systemController,
                                            QueryExecutor<SciDBSystem> queryExecutor, BenchmarkContext benchmarkContext) {
        super(systemController, queryExecutor, benchmarkContext);
    }


    @Override
    public long loadData() throws Exception {
        long totalTime = 0;

        List<String> sliceFilePaths = getSliceFilePaths(benchmarkContext);
        for (int i = 0; i < sliceFilePaths.size(); i++) {
            String arrayName = benchmarkContext.getArrayNameN(i);

            long tileUpperBound = DomainUtil.getDimensionUpperBound(benchmarkContext.getArrayDimensionality(), benchmarkContext.getTileSize() / TYPE_SIZE);
            System.out.println(tileUpperBound);

            String createArray = String.format("CREATE ARRAY %s <v%d:%s> [ d1=0:%d,%d,0, d2=0:%d,%d,0 ]",
                    arrayName, i, TYPE_BASE, BAND_WIDTH - 1, tileUpperBound, BAND_HEIGHT - 1, tileUpperBound);
            queryExecutor.executeTimedQuery(createArray);
            String insertDataQuery = MessageFormat.format("LOAD({0}, ''{1}'', 0, ''({2})'');",
                    arrayName, sliceFilePaths.get(i), TYPE_BASE);

            totalTime += queryExecutor.executeTimedQuery(insertDataQuery);
        }

        return totalTime;
    }

    @Override
    public long dropData() throws Exception {
        long totalTime = 0;

        for (int i = 0; i < ARRAY_NO; i++) {
            String arrayName = benchmarkContext.getArrayNameN(i);
            String dropCollectionQuery = MessageFormat.format("remove({0});", arrayName);
            totalTime += queryExecutor.executeTimedQuery(dropCollectionQuery);
        }

        return totalTime;
    }

//    @Override
//    public long dropData() throws Exception {
//        long totalTime = 0;
//
//        for (int i = 0; i < ARRAY_NO; i++) {
//            String arrayName = benchmarkContext.getArrayNameN(i);
//            String dropCollectionQuery = MessageFormat.format("remove({0});", arrayName);
//            totalTime += queryExecutor.executeTimedQuery(dropCollectionQuery);
//        }
//
//        return totalTime;
//    }
//
//    @Override
//    public long loadData() throws Exception {
//        StopWatch timer = new StopWatch();
//        loadOperationsBenchmarkData();
//        return timer.getElapsedTime();
//    }
//
//    private void loadOperationsBenchmarkData() throws Exception {
//        List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(benchmarkContext.getArraySize());
//        long fileSize = domainGenerator.getFileSize(domainBoundaries);
//
//        long chunkUpperBound = DomainUtil.getDimensionUpperBound(benchmarkContext.getArrayDimensionality(), benchmarkContext.getTileSize());
//        long chunkSize = chunkUpperBound + 1l;
//        dataGenerator = new RandomDataGenerator(fileSize, benchmarkContext.getDataDir());
//        String filePath = dataGenerator.getFilePath();
//
//        StringBuilder createArrayQuery = new StringBuilder();
//        createArrayQuery.append("CREATE ARRAY ");
//        createArrayQuery.append(benchmarkContext.getArrayName());
//        createArrayQuery.append(" <");
//        createArrayQuery.append(benchmarkContext.getArrayName());
//        createArrayQuery.append(":char>");
//        createArrayQuery.append('[');
//
//        boolean isFirst = true;
//        for (int i = 0; i < domainBoundaries.size(); i++) {
//            if (!isFirst) {
//                createArrayQuery.append(",");
//            }
//            isFirst = false;
//            createArrayQuery.append("axis");
//            createArrayQuery.append(i);
//            createArrayQuery.append("=");
//            Pair<Long, Long> axisDomain = domainBoundaries.get(i);
//            createArrayQuery.append(axisDomain.getFirst());
//            createArrayQuery.append(":");
//            createArrayQuery.append(axisDomain.getSecond());
//            createArrayQuery.append(",");
//            createArrayQuery.append(chunkSize);
//            createArrayQuery.append(",");
//            createArrayQuery.append('0');
//        }
//
//        createArrayQuery.append(']');
//
//        queryExecutor.executeTimedQuery(createArrayQuery.toString());
//
//        String insertDataQuery = MessageFormat.format("LOAD {0} FROM ''{1}'' AS ''(char)''", benchmarkContext.getArrayName(), filePath);
//        long insertTime = queryExecutor.executeTimedQuery(insertDataQuery, "-n");
//
//        File resultsDir = IO.getResultsDir();
//        File insertResultFile = new File(resultsDir.getAbsolutePath(), "SciDB_insert_results.csv");
//
//        IO.appendLineToFile(insertResultFile.getAbsolutePath(), String.format("\"%s\", \"%d\", \"%d\", \"%d\", \"%d\"",
//                benchmarkContext.getArrayName(), fileSize, chunkSize, benchmarkContext.getArrayDimensionality(), insertTime));
//    }
}
