package system.rasdaman;


import benchmark.*;
import data.DomainGenerator;
import util.Pair;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author George Merticariu
 */
public class RasdamanQueryGenerator extends QueryGenerator {

    public RasdamanQueryGenerator(BenchmarkContext benchmarkContext) {
        super(benchmarkContext);
    }

    private String getQuery(int dimension, String arrayName, String function, int start, long stop) {
        String subsetQuery;
        List<String> logicalFunctions = new ArrayList<>(Arrays.asList("and", "or", "xor", "not"));
        List<String> aggregateFunctions = new ArrayList<>(Arrays.asList("min_cells", "max_cells", "add_cells", "avg_cells"));

        if (aggregateFunctions.contains(function)) {
            switch (dimension) {
                case 1:
                    subsetQuery = "SELECT " + function + "(c[%d:%d]) FROM %s as c";
                    return String.format(subsetQuery, start, stop, arrayName);
                case 2:
                    subsetQuery = "SELECT " + function + "(c[%d:%d, %d:%d]) FROM %s as c";
                    return String.format(subsetQuery, start, stop, start, stop, arrayName);
                case 3:
                    subsetQuery = "SELECT " + function + "(c[%d:%d, %d:%d, %d:%d]) FROM %s as c";
                    return String.format(subsetQuery, start, stop, start, stop, start, stop, arrayName);
                case 4:
                    subsetQuery = "SELECT " + function + "(c[%d:%d, %d:%d, %d:%d, %d:%d]) FROM %s as c";
                    return String.format(subsetQuery, start, stop, start, stop, start, stop, start, stop, arrayName);
//                case 5:
//                    subsetQuery = "SELECT " + function + "(c[%d:%d, %d:%d, %d:%d, %d:%d, %d:%d]) FROM %s as c";
//                    break;
//                case 6:
//                    subsetQuery = "SELECT " + function + "(c[%d:%d, %d:%d, %d:%d, %d:%d, %d:%d, %d:%d]) FROM %s as c";
//                    break;
//                case 7:
//                    subsetQuery = "SELECT " + function + "(c[%d:%d, %d:%d, %d:%d, %d:%d, %d:%d, %d:%d, %d:%d]) FROM %s as c";
//                    break;
//                case 8:
//                    subsetQuery = "SELECT " + function + "(c[%d:%d, %d:%d, %d:%d, %d:%d, %d:%d, %d:%d, %d:%d, %d:%d]) FROM %s as c";
                default:
                    subsetQuery = "SELECT " + function + "(c[%d:%d, %d:%d]) FROM %s as c";
                    return String.format(subsetQuery, start, stop, start, stop, arrayName);
            }
        }
//        else if (logicalFunctions.contains(function)) {
        else {
            switch (dimension) {
                case 1:
                    subsetQuery = "SELECT (c[%d:%d]) " + function + "(c[%d:%d]) FROM %s as c";
                    return String.format(subsetQuery, start, stop, start, stop, arrayName);
                case 2:
                    subsetQuery = "SELECT (c[%d:%d, %d:%d]) " + function + " (c[%d:%d, %d:%d]) FROM %s as c";
                    return String.format(subsetQuery, start, stop, start, stop, start, stop, start, stop, arrayName);
                case 3:
                    subsetQuery = "SELECT (c[%d:%d, %d:%d, %d:%d]) " + function + " (c[%d:%d, %d:%d, %d:%d]) FROM %s as c";
                    return String.format(subsetQuery, start, stop, start, stop, start, stop, start, stop, start, stop, start, stop, arrayName);
                case 4:
                    subsetQuery = "SELECT ((c[%d:%d, %d:%d, %d:%d, %d:%d]) " + function + " (c[%d:%d, %d:%d, %d:%d, %d:%d])) FROM %s as c";
                    return String.format(subsetQuery, start, stop, start, stop, start, stop, start, stop,
                            start, stop, start, stop, start, stop, start, stop, arrayName);
//                case 5:
//                    subsetQuery = "SELECT " + function + "(c[%d:%d, %d:%d, %d:%d, %d:%d, %d:%d]) FROM %s as c";
//                    break;
//                case 6:
//                    subsetQuery = "SELECT " + function + "(c[%d:%d, %d:%d, %d:%d, %d:%d, %d:%d, %d:%d]) FROM %s as c";
//                    break;
//                case 7:
//                    subsetQuery = "SELECT " + function + "(c[%d:%d, %d:%d, %d:%d, %d:%d, %d:%d, %d:%d, %d:%d]) FROM %s as c";
//                    break;
//                case 8:
//                    subsetQuery = "SELECT " + function + "(c[%d:%d, %d:%d, %d:%d, %d:%d, %d:%d, %d:%d, %d:%d, %d:%d]) FROM %s as c";
//                    break;
                default:
                    subsetQuery = "SELECT (c[%d:%d, %d:%d]) " + function + " (c[%d:%d, %d:%d]) FROM %s as c";
                    return String.format(subsetQuery, start, stop, start, stop, arrayName);
            }
        }
    }

    @Override
    public Benchmark getOperationsBenchmark() {
        Benchmark ret = new Benchmark();
        int arrayDimensionality = benchmarkContext.getArrayDimensionality();
        String arrayName = benchmarkContext.getArrayName();
        DomainGenerator domainGenerator = new DomainGenerator(arrayDimensionality);
        List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(benchmarkContext.getArraySize());
        long upperBoundary = domainBoundaries.get(0).getSecond();

        //TODO joins, nested queries, sorting.
        //SELECT
        {
//            System.out.println("Start test SELECT operation");
            BenchmarkSession benchmarkSession = new BenchmarkSession("SELECT");
            String query = "SELECT c FROM %s AS c";
            benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, arrayName)));
            ret.add(benchmarkSession);
//            System.out.println("Stop test SELECT operation");
        }

        //JOINS
        {
//            System.out.println("Start test SELECT operation");
            BenchmarkSession benchmarkSession = new BenchmarkSession("JOINS");
            String query = "SELECT c FROM %s AS c, %s as d";
            benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, arrayName, arrayName)));
            ret.add(benchmarkSession);
//            System.out.println("Stop test SELECT operation");
        }

        //CASTING
        {
//            System.out.println("Start test SELECT operation");
            BenchmarkSession benchmarkSession = new BenchmarkSession("CASTING");
            String query = "SELECT (float)c FROM %s AS c";
            benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, arrayName)));
            ret.add(benchmarkSession);
//            System.out.println("Stop test SELECT operation");
        }

        {
//            System.out.println("Start test aggregate min, max, add, avg operation");
            String[] aggregateFuncs = {"min_cells", "max_cells", "add_cells", "avg_cells"};
            for (String aggregateFunc : aggregateFuncs) {
//                long upperBoundary = domainBoundaries.get(0).getSecond();
//                System.out.println(domainBoundaries.get(0));

                BenchmarkSession benchmarkSession = new BenchmarkSession(
                        String.format("subset [0, i], with i = [0, %d] and aggregate function: %s", upperBoundary, aggregateFunc));
                for (int i = 0; i <= upperBoundary; i++) {
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(getQuery(arrayDimensionality, arrayName, aggregateFunc, 0, i)));
                }
                ret.add(benchmarkSession);

                benchmarkSession = new BenchmarkSession(
                        String.format("subset [i, %d], with i = [0, %d] and aggregate function: %s", arrayDimensionality, arrayDimensionality, aggregateFunc));
                for (int i = 0; i <= upperBoundary; i++) {
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(getQuery(arrayDimensionality, arrayName, aggregateFunc, i, upperBoundary)));
                }
                ret.add(benchmarkSession);

            }
//            System.out.println("Stop test aggregate min, max, add, avg operation");
        }
//*/
        {
//            System.out.println("Start test trigonometric operations");
            String[][] unaryFuncs = {{"sqrt", "abs(c)"}, {"sin", "c"}, {"cos", "c"}, {"tan", "c"},
//            {"arccos", "c"}, {"acos", "c"}, {"asin", "c"}
                };
            for (String[] unaryFunc : unaryFuncs) {
                String func = unaryFunc[0];
                BenchmarkSession benchmarkSession = new BenchmarkSession(func);
                String query = "SELECT %s FROM %s AS c";
                String expr = unaryFunc[1];
                for (int i = 0; i < 5; i++) {
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, expr, arrayName)));
                    expr = func + "(" + expr + ")";
                }
                ret.add(benchmarkSession);
            }
//            System.out.println("Stop test trigonometric operations");
        }

        {
//            System.out.println("Start test logical operations");
            String[][] logicalFuncs = {{"Logical AND", "and"}, {"Logical OR", "or"}, {"Logical XOR", "xor"}}; //not
            for (String[] logicalFunc : logicalFuncs) {
                String func = logicalFunc[0];
                BenchmarkSession benchmarkSession = new BenchmarkSession(func);
                String expr = logicalFunc[1];
                for (int i = 0; i <= upperBoundary; i++) {
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(getQuery(arrayDimensionality, arrayName, expr, 0, i)));
                }
                ret.add(benchmarkSession);
            }
//            System.out.println("Stop test logical operations");
        }

        {
//            System.out.println("Start test algebraic operations");
            String[][] binaryFuncs = {{"multiplication", "*"}, {"division", "/"}, {"addition", "+"}, {"subtraction", "-"}};
            for (String[] binaryFunc : binaryFuncs) {
                BenchmarkSession benchmarkSession = new BenchmarkSession(binaryFunc[0]);
                String query = "SELECT min_cells(%s) FROM %s AS c";
                String expr = "c";
                String op = binaryFunc[1];
                for (int i = 0; i < 10; i++) {
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, expr, arrayName)));
                    expr = expr + op + "c";
                }
                ret.add(benchmarkSession);
            }
//            System.out.println("Stop test algebraic operations");
        }

        {
//            System.out.println("Start test comparison operation");
            String[][] comparisonFuncs = {{"less than", "<"}, {"less than or equal to", "<="}, {"not equal to", "!="},
                    {"equal to", "="}, {"greater than", ">"}, {"greater than or equal to", ">="}};
            for (String[] comparisonFunc : comparisonFuncs) {
                BenchmarkSession benchmarkSession = new BenchmarkSession(comparisonFunc[0]);
                String query = "SELECT count_cells(%s) FROM %s AS c";
                String expr = "";
                String op = comparisonFunc[1];
                for (int i = 0; i < 10; i++) {
                    String currExpr = "(c" + op + i + ")";
                    if (expr.isEmpty()) {
                        expr = currExpr;
                    } else {
                        expr = expr + " and " + currExpr;
                    }
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, expr, arrayName)));
                }
                ret.add(benchmarkSession);
             }
//            System.out.println("Stop test comparison operation");
        }

        return ret;
    }

//
//    @Override
//    public Benchmark getOperationsBenchmark() {
//        Benchmark ret = new Benchmark();
//
//        {
//            String[] aggregateFuncs = {"min_cells", "max_cells", "add_cells", "avg_cells"};
//            for (String aggregateFunc : aggregateFuncs) {
//                String subsetQuery = "SELECT " + aggregateFunc + "(c[%d:%d,%d:%d]) FROM %s as c";
//                BenchmarkSession benchmarkSession = new BenchmarkSession("subset window lower left to lower right, " + aggregateFunc);
//                for (int i = 0; i < 8; i++) {
//                    int origin = i * 500;
//                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(
//                            subsetQuery, origin, 3999 + origin, 0, 3999, benchmarkContext.getArrayName0())));
//                }
//                ret.add(benchmarkSession);
//                benchmarkSession = new BenchmarkSession("subset window lower left to upper right, " + aggregateFunc);
//                for (int i = 0; i < 8; i++) {
//                    int origin = i * 500;
//                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(
//                            subsetQuery, origin, 3999 + origin, origin, 3999 + origin, benchmarkContext.getArrayName0())));
//                }
//                ret.add(benchmarkSession);
//                benchmarkSession = new BenchmarkSession("subset window zoom in, " + aggregateFunc);
//                for (int i = 0; i < 8; i++) {
//                    int zoom = i * 500;
//                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(
//                            subsetQuery, zoom, 7999 - zoom, zoom, 7999 - zoom, benchmarkContext.getArrayName0())));
//                }
//                ret.add(benchmarkSession);
//                benchmarkSession = new BenchmarkSession("subset window zoom out, " + aggregateFunc);
//                for (int i = 7; i >= 0; i--) {
//                    int zoom = i * 500;
//                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(
//                            subsetQuery, zoom, 7999 - zoom, zoom, 7999 - zoom, benchmarkContext.getArrayName0())));
//                }
//                ret.add(benchmarkSession);
//            }
//        }
//
//        return ret;
//    }


    @Override
    public Benchmark getCachingBenchmark() {
        Benchmark ret = new Benchmark();
        
        {
            BenchmarkSession domainBenchmark = new BenchmarkSession("domain benchmark session");
            // count cloud-free pixels, returning a 1D array for all arrays
            String cloudCoverQuery = String.format("SELECT count_cells(c > 0 and d > 0) FROM %s AS c, %s AS d",
                    benchmarkContext.getArrayName0(), benchmarkContext.getArrayName1());
            domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(cloudCoverQuery));
            // run ndvi
            String ndviQueryFormat = "SELECT count_cells((((nir - red) / (nir + red)) > %f) AND (((nir - red) / (nir + red)) < %f)) FROM %s AS nir, %s AS red";
            domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(String.format(ndviQueryFormat,
                    0.2, 0.4, benchmarkContext.getArrayName0(), benchmarkContext.getArrayName1())));
            domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(String.format(ndviQueryFormat,
                    0.22, 0.45, benchmarkContext.getArrayName0(), benchmarkContext.getArrayName1())));
            domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(String.format(ndviQueryFormat,
                    0.21, 0.38, benchmarkContext.getArrayName0(), benchmarkContext.getArrayName1())));
            // calculate SAVI on a spatial subset
            // savi = ((NIR-R) / (NIR + R + L)) * (1+L)
            String saviQueryFormat = "SELECT min_cells(((nir - red) / (nir + red + 0.5)) * 1.5) FROM %s AS nir, %s AS red";
            domainBenchmark.addBenchmarkQuery(new BenchmarkQuery(String.format(saviQueryFormat,
                    benchmarkContext.getArrayName0(), benchmarkContext.getArrayName1())));
            //ret.add(domainBenchmark);
        }
        
        {
            String[] aggregateFuncs = {"min_cells", "max_cells", "add_cells", "avg_cells"};
            for (String aggregateFunc : aggregateFuncs) {
                String subsetQuery = "SELECT " + aggregateFunc + "(c[%d:%d,%d:%d]) FROM %s as c";
                BenchmarkSession benchmarkSession = new BenchmarkSession("subset window lower left to lower right, " + aggregateFunc);
                for (int i = 0; i < 8; i++) {
                    int origin = i * 500;
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(
                            subsetQuery, origin, 3999 + origin, 0, 3999, benchmarkContext.getArrayName0())));
                }
                ret.add(benchmarkSession);
                benchmarkSession = new BenchmarkSession("subset window lower left to upper right, " + aggregateFunc);
                for (int i = 0; i < 8; i++) {
                    int origin = i * 500;
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(
                            subsetQuery, origin, 3999 + origin, origin, 3999 + origin, benchmarkContext.getArrayName0())));
                }
                ret.add(benchmarkSession);
                benchmarkSession = new BenchmarkSession("subset window zoom in, " + aggregateFunc);
                for (int i = 0; i < 8; i++) {
                    int zoom = i * 500;
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(
                            subsetQuery, zoom, 7999 - zoom, zoom, 7999 - zoom, benchmarkContext.getArrayName0())));
                }
                ret.add(benchmarkSession);
                benchmarkSession = new BenchmarkSession("subset window zoom out, " + aggregateFunc);
                for (int i = 7; i >= 0; i--) {
                    int zoom = i * 500;
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(
                            subsetQuery, zoom, 7999 - zoom, zoom, 7999 - zoom, benchmarkContext.getArrayName0())));
                }
                ret.add(benchmarkSession);
            }
        }
        
        {
            String[] aggregateFuncs = {"count_cells", "all_cells", "some_cells"};
            for (String aggregateFunc : aggregateFuncs) {
                String subsetExpr = "c[%d:%d,%d:%d]";
                String subsetQuery = "SELECT " + aggregateFunc + "({0} > 0.0 and {0} < 100.0) FROM {1} as c";
                BenchmarkSession benchmarkSession = new BenchmarkSession("subset window lower left to lower right, " + aggregateFunc);
                for (int i = 0; i < 8; i++) {
                    int origin = i * 500;
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(MessageFormat.format(subsetQuery,
                            String.format(subsetExpr, origin, 3999 + origin, 0, 3999), benchmarkContext.getArrayName0())));
                }
                ret.add(benchmarkSession);
                benchmarkSession = new BenchmarkSession("subset window lower left to upper right, " + aggregateFunc);
                for (int i = 0; i < 8; i++) {
                    int origin = i * 500;
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(MessageFormat.format(subsetQuery,
                            String.format(subsetExpr, origin, 3999 + origin, origin, 3999 + origin), benchmarkContext.getArrayName0())));
                }
                ret.add(benchmarkSession);
                benchmarkSession = new BenchmarkSession("subset window zoom in, " + aggregateFunc);
                for (int i = 0; i < 8; i++) {
                    int zoom = i * 500;
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(MessageFormat.format(subsetQuery,
                            String.format(subsetExpr, zoom, 7999 - zoom, zoom, 7999 - zoom), benchmarkContext.getArrayName0())));
                }
                ret.add(benchmarkSession);
                benchmarkSession = new BenchmarkSession("subset window zoom out, " + aggregateFunc);
                for (int i = 7; i >= 0; i--) {
                    int zoom = i * 500;
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(MessageFormat.format(subsetQuery,
                            String.format(subsetExpr, zoom, 7999 - zoom, zoom, 7999 - zoom), benchmarkContext.getArrayName0())));
                }
                ret.add(benchmarkSession);
            }
        }
        
        {
            String[][] comparisonFuncs = {{"less than", "<"}, {"greater than", ">"}, {"less than or equal to", "<="}, 
            {"greater than or equal to", ">="}, {"equal to", "="}, {"not equal to", "!="}};
            for (String[] comparisonFunc : comparisonFuncs) {
                BenchmarkSession benchmarkSession = new BenchmarkSession(comparisonFunc[0]);
                String query = "SELECT count_cells(%s) FROM %s AS c";
                String expr = "";
                String op = comparisonFunc[1];
                for (int i = 0; i < 10; i++) {
                    String currExpr = "(c" + op + i + ")";
                    if (expr.isEmpty()) {
                        expr = currExpr;
                    } else {
                        expr = expr + " and " + currExpr;
                    }
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, expr, benchmarkContext.getArrayName0())));
                }
                ret.add(benchmarkSession);
            }
        }
        
        {
            String[][] unaryFuncs = {{"sqrt", "abs(c)"}, {"sin", "c"}, {"cos", "c"}};
            for (String[] unaryFunc : unaryFuncs) {
                String func = unaryFunc[0];
                BenchmarkSession benchmarkSession = new BenchmarkSession(func);
                String query = "SELECT min_cells(%s) FROM %s AS c";
                String expr = unaryFunc[1];
                for (int i = 0; i < 10; i++) {
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, expr, benchmarkContext.getArrayName0())));
                    expr = func + "(" + expr + ")";
                }
                ret.add(benchmarkSession);
            }
        }
        
        {
            String[][] binaryFuncs = {{"multiplication", "*"}, {"division", "/"}, {"addition", "+"}, {"subtraction", "-"}};
            for (String[] binaryFunc : binaryFuncs) {
                BenchmarkSession benchmarkSession = new BenchmarkSession(binaryFunc[0]);
                String query = "SELECT min_cells(%s) FROM %s AS c";
                String expr = "c";
                String op = binaryFunc[1];
                for (int i = 0; i < 10; i++) {
                    benchmarkSession.addBenchmarkQuery(new BenchmarkQuery(String.format(query, expr, benchmarkContext.getArrayName0())));
                    expr = expr + op + "c";
                }
                ret.add(benchmarkSession);
            }
        }
        
        return ret;
    }

    @Override
    public Benchmark getStorageBenchmark() {
        Benchmark ret = new Benchmark();

        List<List<Pair<Long, Long>>> sizeQueryDomain = queryDomainGenerator.getSizeQueryDomain();
        List<List<Pair<Long, Long>>> positionQueryDomain = queryDomainGenerator.getPositionQueryDomain();
        List<List<Pair<Long, Long>>> shapeQueryDomain = queryDomainGenerator.getShapeQueryDomain();
        List<Pair<List<Pair<Long, Long>>, List<Pair<Long, Long>>>> multiAccessQueryDomain = queryDomainGenerator.getMultiAccessQueryDomain();

        for (List<Pair<Long, Long>> queryDomain : sizeQueryDomain) {
            ret.add(BenchmarkQuery.size(generateRasdamanQuery(queryDomain), benchmarkContext.getArrayDimensionality()));
        }

        for (List<Pair<Long, Long>> queryDomain : positionQueryDomain) {
            ret.add(BenchmarkQuery.position(generateRasdamanQuery(queryDomain), benchmarkContext.getArrayDimensionality()));
        }

        for (List<Pair<Long, Long>> queryDomain : shapeQueryDomain) {
            ret.add(BenchmarkQuery.shape(generateRasdamanQuery(queryDomain), benchmarkContext.getArrayDimensionality()));
        }

        for (Pair<List<Pair<Long, Long>>, List<Pair<Long, Long>>> multiAccessDomains : multiAccessQueryDomain) {
            ret.add(BenchmarkQuery.multipleSelect(generateMultiDomainQuery(multiAccessDomains.getFirst(), 
                    multiAccessDomains.getSecond()), benchmarkContext.getArrayDimensionality()));
        }
        
        List<Pair<Long, Long>> middlePointQueryDomain = queryDomainGenerator.getMiddlePointQueryDomain();
        ret.add(BenchmarkQuery.middlePoint(generateRasdamanQuery(middlePointQueryDomain), benchmarkContext.getArrayDimensionality()));

        return ret;
    }

    public static String convertToRasdamanDomain(List<Pair<Long, Long>> domain) {
        StringBuilder rasdamanDomain = new StringBuilder();
        rasdamanDomain.append('[');

        boolean isFirst = true;
        for (Pair<Long, Long> axisDomain : domain) {
            if (!isFirst) {
                rasdamanDomain.append(",");
            }

            rasdamanDomain.append(axisDomain.getFirst());
            rasdamanDomain.append(':');
            rasdamanDomain.append(axisDomain.getSecond());
            isFirst = false;
        }

        rasdamanDomain.append(']');

        return rasdamanDomain.toString();
    }


    private String generateMultiDomainQuery(List<Pair<Long, Long>> domain1, List<Pair<Long, Long>> domain2) {
        return MessageFormat.format("SELECT count_cells({0}{1} >= 0) + count_cells({0}{2} >= 0) FROM {0}", 
                benchmarkContext.getArrayName(), convertToRasdamanDomain(domain1), convertToRasdamanDomain(domain2));
    }

    private String generateRasdamanQuery(List<Pair<Long, Long>> domain) {
        return MessageFormat.format("SELECT {0}{1} FROM {0}", benchmarkContext.getArrayName(), convertToRasdamanDomain(domain));
    }
}