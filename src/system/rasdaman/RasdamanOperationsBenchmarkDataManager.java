package system.rasdaman;

import benchmark.BenchmarkContext;
import benchmark.operations.OperationsBenchmarkContext;
import benchmark.operations.OperationsBenchmarkDataManager;
import data.RandomDataGenerator;
import util.DomainUtil;
import util.IO;
import util.Pair;
import util.StopWatch;

import java.io.File;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by danut on 23.03.17.
 */
public class RasdamanOperationsBenchmarkDataManager extends OperationsBenchmarkDataManager<RasdamanSystem> {

    private static final int TYPE_SIZE = 8;
    private static final String TYPE_MDD = "DoubleImage";
    private static final String TYPE_SET = "DoubleSet";

    private final RasdamanTypeManager typeManager;


    public RasdamanOperationsBenchmarkDataManager(RasdamanSystem systemController,
                                               RasdamanQueryExecutor queryExecutor, BenchmarkContext benchmarkContext) {
        super(systemController, queryExecutor, benchmarkContext);
        typeManager = new RasdamanTypeManager(queryExecutor);
    }

//    @Override
//    public long loadData() throws Exception {
//        long totalTime = 0;
//
//        List<String> sliceFilePaths = getSliceFilePaths(benchmarkContext);
//        for (int i = 0; i < sliceFilePaths.size(); i++) {
//            String arrayName = benchmarkContext.getArrayNameN(i);
//
//            queryExecutor.executeTimedQuery(String.format("CREATE COLLECTION %s %s", arrayName, TYPE_SET));
//
//            long tileUpperBound = DomainUtil.getDimensionUpperBound(benchmarkContext.getArrayDimensionality(), benchmarkContext.getTileSize() / TYPE_SIZE);
//            String insertQuery = String.format("INSERT INTO %s VALUES $1 TILING REGULAR [0:%d,0:%d] TILE SIZE %d",
//                    arrayName, tileUpperBound - 1, tileUpperBound - 1, tileUpperBound * tileUpperBound * TYPE_SIZE);

//    String insertQuery = String.format("INSERT INTO %s VALUES $1 TILING ALIGNED %s TILE SIZE %d",
//            benchmarkContext.getArrayName(), RasdamanQueryGenerator.convertToRasdamanDomain(tileStructureDomain), tileSize);
//            String mddDomain = String.format("[0:%d,0:%d]", BAND_WIDTH - 1, BAND_HEIGHT - 1);
//            totalTime += queryExecutor.executeTimedQuery(insertQuery,
//                    "-f", sliceFilePaths.get(i),
//                    "--mdddomain", mddDomain,
//                    "--mddtype", TYPE_MDD
//            );
//        }
//
//        return totalTime;
//    }

    @Override
    public long loadData() throws Exception {
        StopWatch timer = new StopWatch();
        loadOperationsBenchmarkData();
        return timer.getElapsedTime();
    }

    private void loadOperationsBenchmarkData() throws Exception {
        int slices = (int) (benchmarkContext.getArraySize() / (DomainUtil.SIZE_1GB));

//        boolean startSequentialUpdate = false;
//        if (benchContext.getCollSize() > DomainUtil.SIZE_1GB) {
//            benchContext.setCollSize(DomainUtil.SIZE_1GB);
//            startSequentialUpdate = true;
//        }

        long insertTime = -1;

        List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(benchmarkContext.getArraySize());
        long fileSize = domainGenerator.getFileSize(domainBoundaries);

        System.out.println(fileSize);

        long chunkSize = DomainUtil.getDimensionUpperBound(benchmarkContext.getArrayDimensionality(), ((OperationsBenchmarkContext)benchmarkContext).getTileSize());
        long tileSize = (long) Math.pow(chunkSize + 1l, benchmarkContext.getArrayDimensionality());
        tileSize = fileSize * 8;

        dataGenerator = new RandomDataGenerator(fileSize, benchmarkContext.getDataDir());
        String filePath = dataGenerator.getFilePath();

        List<Pair<Long, Long>> tileStructureDomain = new ArrayList<>();
        for (int i = 0; i < benchmarkContext.getArrayDimensionality(); i++) {
            tileStructureDomain.add(Pair.of(0l, chunkSize));
        }

//        typeManager.deleteTypes(typeManager.getBaseTypeName("char"));
        Pair<String, String> aChar = typeManager.createOperationsType(benchmarkContext.getArrayDimensionality(), "char");

        String createCollectionQuery = String.format("CREATE COLLECTION %s %s", benchmarkContext.getArrayName(), aChar.getSecond());
        queryExecutor.executeTimedQuery(createCollectionQuery);

        String insertQuery = String.format("INSERT INTO %s VALUES $1 TILING REGULAR %s TILE SIZE %d",
                benchmarkContext.getArrayName(), RasdamanQueryGenerator.convertToRasdamanDomain(domainBoundaries), tileSize);
        System.out.println("Executing insert query: " + insertQuery);
        insertTime = queryExecutor.executeTimedQuery(insertQuery,
                "--mddtype", aChar.getFirst(),
                "--mdddomain", RasdamanQueryGenerator.convertToRasdamanDomain(domainBoundaries),
                "--file", filePath);

        File resultsDir = IO.getResultsDir();
        File insertResultFile = new File(resultsDir.getAbsolutePath(), "rasdaman_insert_results.csv");

//        if (startSequentialUpdate) {
//            insertTime = updateCollection(slices);
//        }

        IO.appendLineToFile(insertResultFile.getAbsolutePath(), String.format("\"%s\", \"%d\", \"%d\", \"%d\", \"%d\"",
                benchmarkContext.getArrayName(), fileSize*slices, chunkSize + 1l, benchmarkContext.getArrayDimensionality(), insertTime));

//        return insertTime;
    }

    @Override
    public long dropData() throws Exception {
        long ret = 0;
        for (int i = 0; i < ARRAY_NO; i++) {
            String arrayName = benchmarkContext.getArrayNameN(i);
            String dropQuery = MessageFormat.format("DROP COLLECTION {0}", arrayName);
            ret += queryExecutor.executeTimedQuery(dropQuery);
        }
        return ret;
    }
}
