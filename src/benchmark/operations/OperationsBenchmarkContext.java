package benchmark.operations;

import benchmark.BenchmarkContext;
import benchmark.BenchmarkQuery;

/**
 * Created by danut on 23.03.17.
 */
public class OperationsBenchmarkContext extends BenchmarkContext {

        private static final int REPEAT_QUERIES_ONCE = 1;

        protected long dataSize;

        public OperationsBenchmarkContext(String dataDir) {
            super(REPEAT_QUERIES_ONCE, dataDir, -1, TYPE_OPERATIONS);
            arrayDimensionality = 2;
            arraySize = OperationsBenchmarkDataManager.ARRAY_SIZE;
            arraySizeShort = OperationsBenchmarkDataManager.ARRAY_SIZE_SHORT;
            cleanQuery = false;
            updateArrayName();
        }

        public long getDataSize() {
            return dataSize;
        }

        public void setDataSize(long dataSize) {
            this.dataSize = dataSize;
        }

        @Override
        public String getBenchmarkSpecificHeader() {
            return "Query, Data size, Execution time (ms), ";
        }

        @Override
        public String getBenchmarkResultLine(BenchmarkQuery query) {
            return String.format("\"%s\", %s, ", query.getQueryString(), getDataSize());
        }

}