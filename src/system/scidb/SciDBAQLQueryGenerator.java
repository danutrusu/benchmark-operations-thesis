package system.scidb;

import benchmark.*;
import data.DomainGenerator;
import util.Pair;

import java.text.MessageFormat;
import java.util.List;

/**
 * @author George Merticariu
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public class SciDBAQLQueryGenerator extends QueryGenerator {

    public SciDBAQLQueryGenerator(BenchmarkContext benchmarkContext) {
        super(benchmarkContext);
    }

    @Override
    public Benchmark getOperationsBenchmark() {
        Benchmark ret = new Benchmark();
        int arrayDimensionality = benchmarkContext.getArrayDimensionality();
        String arrayName = benchmarkContext.getArrayName();
        DomainGenerator domainGenerator = new DomainGenerator(arrayDimensionality);
        List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(benchmarkContext.getArraySize());
        long upperBoundary = domainBoundaries.get(0).getSecond();

        System.out.println(arrayDimensionality + "  " + upperBoundary + " " + benchmarkContext.getArraySize());

        {
            BenchmarkSession benchmarkSession = new BenchmarkSession("SELECT");
            String query = "SELECT * FROM %s AS c";
            benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, arrayName)));
            ret.add(benchmarkSession);
        }

        {
            BenchmarkSession benchmarkSession = new BenchmarkSession("JOINS");
            String query = "SELECT * FROM %s AS c, %s AS d";
            benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, arrayName, arrayName)));
            ret.add(benchmarkSession);
        }

        {
            BenchmarkSession benchmarkSession = new BenchmarkSession("casting from double to float");
            String query = "SELECT float(v) FROM %s AS c";
            benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, arrayName)));
            ret.add(benchmarkSession);
        }


        {
            String[] aggregateFuncs = {"min", "max", "sum", "avg", "prod", "count", "var"};
            for (String aggregateFunc : aggregateFuncs) {

                BenchmarkSession benchmarkSession = new BenchmarkSession(
                        String.format("aggregate function: %s  on each dimension (out of %d)", aggregateFunc, arrayDimensionality));
                for (int i = 0; i < arrayDimensionality; i++) {
                    String query = String.format("SELECT %s(d%d) FROM %s", aggregateFunc, i + 1, arrayName);
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(query));
                }
                ret.add(benchmarkSession);
            }
        }

        {
            String[] algebraicFuncs1 = {"sqrt(abs", "abs"};
            for (String algebraicFunc : algebraicFuncs1) {

                BenchmarkSession benchmarkSession = new BenchmarkSession(
                        String.format("algebraic function: %s  on each dimension (out of %d)", algebraicFunc, arrayDimensionality));
                for (int i = 0; i < arrayDimensionality; i++) {
                    String query = String.format("SELECT %s(d%d) FROM %s", algebraicFunc, i + 1, arrayName);
                    if (algebraicFunc.equals("sqrt(abs"))
                        query = String.format("SELECT %s(d%d)) FROM %s", algebraicFunc, i + 1, arrayName);

                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(query));
                }
                ret.add(benchmarkSession);
            }

            String[] algebraicFuncs2 = {"+", "-", "*", "/", "%"};
            for (String algebraicFunc : algebraicFuncs2) {

                BenchmarkSession benchmarkSession = new BenchmarkSession(
                        String.format("algebraic function 2 : %s  on each dimension (out of %d)", algebraicFunc, arrayDimensionality));
                for (int i = 0; i < arrayDimensionality; i++) {
                    String query = String.format("SELECT d%d %s d%d FROM %s", i + 1, algebraicFunc, i + 1, arrayName);

                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(query));
                }
                ret.add(benchmarkSession);
            }
        }

        return ret;

    }
        @Override
    public Benchmark getStorageBenchmark() {

        Benchmark queries = new Benchmark();

        List<List<Pair<Long, Long>>> sizeQueryDomain = queryDomainGenerator.getSizeQueryDomain();
        List<List<Pair<Long, Long>>> positionQueryDomain = queryDomainGenerator.getPositionQueryDomain();
        List<List<Pair<Long, Long>>> shapeQueryDomain = queryDomainGenerator.getShapeQueryDomain();
        List<Pair<List<Pair<Long, Long>>, List<Pair<Long, Long>>>> multiAccessQueryDomain = queryDomainGenerator.getMultiAccessQueryDomain();

        for (List<Pair<Long, Long>> queryDomain : sizeQueryDomain) {
            queries.add(BenchmarkQuery.size(generateSciDBQuery(queryDomain), benchmarkContext.getArrayDimensionality()));
        }

        for (List<Pair<Long, Long>> queryDomain : positionQueryDomain) {
            queries.add(BenchmarkQuery.position(generateSciDBQuery(queryDomain), benchmarkContext.getArrayDimensionality()));
        }

        for (List<Pair<Long, Long>> queryDomain : shapeQueryDomain) {
            queries.add(BenchmarkQuery.shape(generateSciDBQuery(queryDomain), benchmarkContext.getArrayDimensionality()));
        }

        for (Pair<List<Pair<Long, Long>>, List<Pair<Long, Long>>> multiAccessDomains : multiAccessQueryDomain) {
            queries.add(BenchmarkQuery.multipleSelect(generateMultiDomainQuery(multiAccessDomains.getFirst(), multiAccessDomains.getSecond()), benchmarkContext.getArrayDimensionality()));
        }
        
        List<Pair<Long, Long>> middlePointQueryDomain = queryDomainGenerator.getMiddlePointQueryDomain();
        queries.add(BenchmarkQuery.middlePoint(generateSciDBQuery(middlePointQueryDomain), benchmarkContext.getArrayDimensionality()));

        return queries;
    }

    private String generateSciDBQuery(List<Pair<Long, Long>> domain) {
        return MessageFormat.format("SELECT * FROM consume((SELECT a FROM {0} WHERE {1}))", benchmarkContext.getArrayName(), convertToSciDBDomain(domain));
    }

    private String generateMultiDomainQuery(List<Pair<Long, Long>> domain1, List<Pair<Long, Long>> domain2) {
        return MessageFormat.format("SELECT * FROM consume((SELECT count(a) FROM {0} WHERE {1}))", benchmarkContext.getArrayName(), convertToSciDBDomain(domain1, domain2));
    }

    public static String convertToSciDBDomain(List<Pair<Long, Long>> domain) {
        StringBuilder scidbDomain = new StringBuilder();

        boolean isFirst = true;
        int i = 0;
        for (Pair<Long, Long> axisDomain : domain) {
            if (!isFirst) {
                scidbDomain.append(" AND ");
            }

            scidbDomain.append("axis");
            scidbDomain.append(i);
            scidbDomain.append(">=");
            scidbDomain.append(axisDomain.getFirst());
            scidbDomain.append(" AND ");

            scidbDomain.append("axis");
            scidbDomain.append(i);
            scidbDomain.append("<=");
            scidbDomain.append(axisDomain.getSecond());

            isFirst = false;
            ++i;
        }

        return scidbDomain.toString();
    }

    public static String convertToSciDBDomain(List<Pair<Long, Long>> domain1, List<Pair<Long, Long>> domain2) {
        StringBuilder scidbDomain = new StringBuilder();
        scidbDomain.append("(");
        scidbDomain.append(convertToSciDBDomain(domain1));
        scidbDomain.append(")");

        scidbDomain.append(" OR ");

        scidbDomain.append("(");
        scidbDomain.append(convertToSciDBDomain(domain2));
        scidbDomain.append(")");

        return scidbDomain.toString();
    }
}
