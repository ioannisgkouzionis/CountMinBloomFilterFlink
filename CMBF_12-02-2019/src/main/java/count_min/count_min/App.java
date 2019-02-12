package count_min.count_min;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import static java.lang.Math.abs;


public class App
{
    //public static Count_Min sketch= new Count_Min(7,10000,4);   //experimentally;
    private static Count_Min sketch= new Count_Min(0.0002, 0.99,4);

    private static int elementCount = 100000; // Number of elements to test
    private static BloomFilter<String> bf = new BloomFilter<>(0.5, elementCount); // use 0.1 or 0.2

    private static ConcurrentHashMap<Integer, Integer> freq= new ConcurrentHashMap<>(); //used for compute the error
    public static float qcount, error, totalError;

    public static void main( String[] args ) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); //set an execution environment for Flink

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        int num;

        env.getParallelism();  //set parallelism
        env.enableCheckpointing(5000);  // create a checkpoint every 5 seconds
        env.getConfig().setGlobalJobParameters(params);

        if (params.has("sketch")) {
            num = params.getInt("sketch");

        }else num=2;

        if(num==1){
            System.out.println("You have chosen Count-min");
            DataStream<String> inputData;
            if (params.has("inputCM")) {
                System.out.println("Got your input path ");
                inputData = env.readTextFile(params.get("inputCM"));
            } else {
                System.out.println("Executing Count Min with default input data set.");
                inputData = env.readTextFile("DataCM.txt");
            }

            DataStream<Tuple2<Integer, Integer>> data = inputData
                    .map(new CMmapFunction())
                    .keyBy(0)
                    .sum(1);

            DataStream<String> inputQueries;
            if (params.has("inputQCM")) {
                System.out.println("Got your query path");
                inputQueries = env.readTextFile(params.get("inputQCM"));
            } else {
                System.out.println("Executing Count Min with default data set of queries.");
                inputQueries = env.readTextFile("Queries.txt");
            }

            DataStream<Tuple2<Integer, Integer>> queries = inputQueries.map(new CMQmapFunction())
                        .keyBy(0);

            if (params.has("outputCM")) {
                System.out.println("Got your output path");
                queries.writeAsText(params.get("outputCM"), FileSystem.WriteMode.OVERWRITE);
//                queries.print();
            } else {
                System.out.println("Writing Count Min to default output.");
//                queries.print();
                queries.writeAsText("CMoutput.txt", FileSystem.WriteMode.OVERWRITE);
            }

        }
        else {
            System.out.println("You have chosen Bloom Filter");
            DataStream<String> inputData;
            if (params.has("inputBF")) {
                System.out.println("Got your input path ");
                inputData = env.readTextFile(params.get("inputBF"));
            } else {
                System.out.println("Executing Bloom Filter with default input data set.");
                inputData = env.readTextFile("inputBF.txt");
            }

            inputData.map(new BFMapFunction());
            DataStream<Tuple2<String, Boolean>> inputQueries;

            if (params.has("inputQBF")) {
                System.out.println("Got your query path ");
                inputQueries = env.readTextFile(params.get("inputQBF"))
                        .map(new BFQMapFunction())
                        .keyBy(0);
            } else {
                System.out.println("Executing Bloom Filter with default data set of queries.");
                inputQueries = env.readTextFile("inputQBF.txt")
                        .map(new BFQMapFunction())
                        .keyBy(0);
            }

            if (params.has("outputBF")) {
                System.out.println("Got your output path ");
                inputQueries.writeAsText(params.get("outputBF"),FileSystem.WriteMode.OVERWRITE);
            } else {
                System.out.println("Writing Bloom Filter to default output.");
                inputQueries.writeAsText("BFoutput",FileSystem.WriteMode.OVERWRITE);
            }

        }

        // execute program and measure the time execution
        JobExecutionResult result = env.execute("Streaming Count_Min");
        System.out.println("The job took " + result.getNetRuntime(TimeUnit.MILLISECONDS) + " miliseconds to execute");

        if(num==1){
            System.out.println("The number of misses is " + totalError);
            System.out.println("The total error is " + totalError / qcount);
            System.out.println("The number of times that overestimate are " + error);
        }
        else {
            System.out.println("The total error is " + error / qcount);
            System.out.println("The number of times that did wrong " + error);
        }

    }

    public static final class CMmapFunction implements MapFunction<String, Tuple2<Integer,Integer>> {

        public Tuple2<Integer,Integer> map(String value) {
            String[] tokens = value.split("\n");
            int arg1=0;
            int arg2=0;
            for (String token : tokens) {
                if (token.length() > 0) {
                    String[] ar = token.split(",");
                    arg1 = Integer.parseInt(ar[0]);
                    arg2 = Integer.parseInt(ar[1]);

                    sketch.add(arg1, arg2); //insert the data to the table

                    if (freq.containsKey(arg1)) {   //compute the actual data
                        freq.put(arg1, freq.get(arg1) + 1);
                    } else {
                        freq.put(arg1, 1);
                    }
                }
            }
            return new Tuple2(arg1,arg2);

        }
    }


    public static final class CMQmapFunction implements MapFunction<String, Tuple2<Integer, Integer>> {

        public Tuple2<Integer, Integer> map(String value) {
            String[] tokens = value.split("\n");
            int arg1=0;
            int arg2=0;
            for (String token : tokens) {
                if (token.length() > 0) {
                    String[] ar = token.split(",");
                    arg1 = Integer.parseInt(ar[0]);
                    arg2 = (int) sketch.estimateCount(arg1);    //estimate the count number of elements

                    if(freq.containsKey(arg1)) {    //estimate the error
                        qcount++;
                        if ((abs(arg2 - freq.get(arg1))) !=0) {
                            error++;
                            totalError = totalError + abs(arg2 - freq.get(arg1));

                        }
                    }
                }
            }
            return new Tuple2(arg1,arg2);

        }

    }

    public static final class BFMapFunction implements MapFunction<String, Tuple1<String>> {

        public Tuple1<String> map(String value) {
            String[] tokens = value.split("\n");
            String arg1 = null;
            for (String token : tokens) {
                if (token.length() > 0) {
                    arg1 = token;

                    if (freq.containsKey(arg1)) {   //compute the actual data
                        freq.put(Integer.parseInt(arg1), freq.get(arg1) + 1);
                    } else {
                        freq.put(Integer.parseInt(arg1), 1);
                    }

                    bf.add(arg1);   //add the element to the table
                }
            }
            return new Tuple1(arg1);

        }
    }

    public static final class BFQMapFunction implements MapFunction<String, Tuple2<String, Boolean>> {

        public Tuple2<String, Boolean> map(String value) {
            String[] tokens = value.split("\n");
            String arg1 = null;
            boolean arg2 = false;
            for (String token : tokens) {
                if (token.length() > 0) {
                    arg1 = token;
                    arg2 = bf.contains(arg1);   //check if the element exist
                    qcount++;
                    if (arg2){
                        error++;
                    }
                }
            }
            return new Tuple2(arg1,arg2);
        }
    }


}

