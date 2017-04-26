package system.scidb;

import benchmark.BenchmarkContext;
import benchmark.QueryExecutor;
import benchmark.operations.OperationsBenchmarkDataManager;
import util.DomainUtil;
import util.IO;

import java.text.MessageFormat;

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

        String arrayName = benchmarkContext.getArrayName();
        String sliceFilePath = IO.concatPaths(benchmarkContext.getDataDir(), arrayName);

        if (!IO.fileExists(sliceFilePath)) {
            return 0;
        }

        long tileUpperBound = DomainUtil.getDimensionUpperBound(benchmarkContext.getArrayDimensionality(), benchmarkContext.getTileSize() / TYPE_SIZE);
        System.out.println(tileUpperBound);

        String createArray = String.format("CREATE ARRAY %s<v:%s>[d1=0:%d,%d,0, d2=0:%d,%d,0];",
                arrayName, TYPE_BASE, BAND_WIDTH - 1, tileUpperBound, BAND_HEIGHT - 1, tileUpperBound);

        queryExecutor.executeTimedQuery(createArray);
        String insertDataQuery = MessageFormat.format("LOAD({0}, ''{1}'', 0, ''({2})'');",
                arrayName, sliceFilePath, TYPE_BASE);

        totalTime += queryExecutor.executeTimedQuery(insertDataQuery);
        return totalTime;
    }

    @Override
    public long dropData() throws Exception {
        long totalTime = 0;

        String arrayName = benchmarkContext.getArrayName();
        String dropCollectionQuery = MessageFormat.format("remove({0});", arrayName);
        totalTime += queryExecutor.executeTimedQuery(dropCollectionQuery);

        return totalTime;
    }

}
