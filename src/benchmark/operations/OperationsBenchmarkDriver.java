package benchmark.operations;

import benchmark.AdbmsSystem;
import benchmark.Driver;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.SimpleJSAP;

import java.io.IOException;

/**
 * Created by danut on 23.03.17.
 */
public class OperationsBenchmarkDriver extends Driver {

    @Override
    protected SimpleJSAP getCmdLineConfig(Class c) throws JSAPException {
        SimpleJSAP jsap = getCommonCmdLineConfig(c);
        return jsap;
    }

    @Override
    protected int runBenchmark(JSAPResult config) throws IOException {
        int exitCode = 0;

        OperationsBenchmarkContext benchmarkContext = new OperationsBenchmarkContext(config.getString("datadir"));
        benchmarkContext.setLoadData(config.getBoolean("load"));
        benchmarkContext.setDropData(config.getBoolean("drop"));
        benchmarkContext.setGenerateData(config.getBoolean("generate"));
        benchmarkContext.setDisableBenchmark(config.getBoolean("nobenchmark"));
        benchmarkContext.setDisableSystemRestart(config.getBoolean("norestart"));
        benchmarkContext.setCleanQuery(config.getBoolean("cleanquery"));

        String[] systems = config.getStringArray("system");
        String[] configs = config.getStringArray("config");
        if (systems.length != configs.length) {
            throw new IllegalArgumentException(systems.length + " systems specified, but " + configs.length + " system configuration files.");
        }

        long[] dataSizes = config.getLongArray("cacheSizes");
        if (config.contains("tilesize")) {
            long tileSize = config.getLong("tilesize");
            benchmarkContext.setTileSize(tileSize);
        }

        int configInd = 0;
        for (String system : systems) {
            String configFile = configs[configInd++];
            AdbmsSystem adbmsSystem = AdbmsSystem.getAdbmsSystem(system, configFile, benchmarkContext);
            if (benchmarkContext.isDisableBenchmark()) {
                exitCode += runBenchmark(benchmarkContext, adbmsSystem);
            } else {
                ////TODO: modify for operations with different sizes.
                for (long dataSize : dataSizes) {
                    benchmarkContext.setDataSize(dataSize);
                    adbmsSystem.setSystemCacheSize(dataSize);
                    log.info("Data size set to " + dataSize + " bytes in " + adbmsSystem.getSystemName() + ".");
                    exitCode += runBenchmark(benchmarkContext, adbmsSystem);
                }
            }
        }
        return exitCode;
    }

    public static void main(String... args) throws Exception {
        OperationsBenchmarkDriver driver = new OperationsBenchmarkDriver();
        System.exit(driver.runMain(driver, args));
    }

    @Override
    protected String getDescription() {
        return "Benchmark operations behaviour in Array Databases. Currently supported systems: rasdaman.";
    }
}
