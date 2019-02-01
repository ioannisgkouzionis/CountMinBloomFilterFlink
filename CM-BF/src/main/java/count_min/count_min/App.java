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

import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;


import static java.lang.Math.abs;


public class App
{
    //public static Count_Min sketch= new Count_Min(7,10000,4);   //experimentally;
    private static Count_Min sketch= new Count_Min(0.0002, 0.99,4);

    private static int elementCount = 100000; // Number of elements to test
    private static BloomFilter<String> bf = new BloomFilter<>(0.0001, elementCount);

    private static ConcurrentHashMap<Integer, Integer> freq= new ConcurrentHashMap<>(); //used for compute the error
    private static float qcount, error, totalError;

    public static void main( String[] args ) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); //set an execution environment for Flink

//        if (args.length < 4) {
//            args=new String[4];
//
//            args[0]="4";
//            args[1]="1";
//            args[2]="DataBF";
//            args[3]="DataCM";
//
//
//        }


        //env.setParallelism(Integer.parseInt(args[0]));  //set parallelism
        env.setParallelism(4);  //set parallelism
        env.getConfig().setGlobalJobParameters(params);


		/*final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served every second */

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //DataStream<Count_Min> text =env.addSource(new Count_Min_Source("dataaaaaaaaaaa.txt", 1,2)) ;


       // int num=Integer.parseInt(args[1]);
        int num;
        do {
            System.out.println("This is a project on sketches at Apache Flink. Press 1 for Count-Min Sketch or 2 for Bloom Filter sketch");
            Scanner scan = new Scanner(System.in);
            num = scan.nextInt();
            System.out.println("You chose " + num);
        } while (num != 1 & num != 2);

        if(num==1){
            DataStream<String> inputData = env.readTextFile("DataCM.txt");

            //            Read from HDFS
            // read text file from a HDFS running at nnHost:nnPort
            // DataSet<String> hdfsLines = env.readTextFile("hdfs://nnHost:nnPort/path/to/my/textfile");
            //DataStream<String> inputDataHDFS = env.readTextFile("");

            DataStream<Tuple2<Integer, Integer>> data = inputData.map(new CMmapFunction()).keyBy(0).sum(1);

            DataStream<String> inputQueries = env.readTextFile("Queries.txt");

            DataStream<Tuple2<Integer, Integer>> queries = inputQueries.map(new CMQmapFunction()).keyBy(0);
            queries.writeAsText("CMoutput.txt", FileSystem.WriteMode.OVERWRITE);
            //        Write on HDFS
            // write DataSet to a file on a HDFS with a namenode running at nnHost:nnPort
            // textData.writeAsText("hdfs://nnHost:nnPort/my/result/on/localFS");
            queries.writeAsText("");
            queries.print();
        }
        else {
            DataStream<Tuple1<String>> inputData = env.readTextFile("DataBF.txt").map(new BFMapFunction());
            DataStream<Tuple2<String, Boolean>> inputQueries =env.readTextFile("BFErrorQueries.txt").map(new BFQMapFunction()).keyBy(0);
            inputQueries.writeAsText("BFoutput.txt", FileSystem.WriteMode.OVERWRITE);

            //        Write on HDFS
            // write DataSet to a file on a HDFS with a namenode running at nnHost:nnPort
            // textData.writeAsText("hdfs://nnHost:nnPort/my/result/on/localFS");
            inputQueries.writeAsText("");

        }

        // execute program and mesure the time execution
        JobExecutionResult result = env.execute("Streaming Count_Min");
        System.out.println("The job took " + result.getNetRuntime(TimeUnit.MILLISECONDS) + " miliseconds to execute");

        if(num==1){

        System.out.println("The number of misses is " + totalError);
        System.out.println("The total error is " + totalError / qcount);
        System.out.println("The number of times that overestimate are " + error);
        }
        else {
            double bitsperelement =bf.getBitsPerElement();
            double expectedFalsePositiveProbability= bf.expectedFalsePositiveProbability();
//            double getFalsePositiveProbability= bf.getFalsePositiveProbability();
//            System.out.println("expectedFalsePositiveProbability " + expectedFalsePositiveProbability);
//            System.out.println("The actual number getFalsePositiveProbability " + getFalsePositiveProbability);
            System.out.println("The actual number of bits per element " + bitsperelement);
            System.out.println("The total error is " + error / qcount);
            System.out.println("The number of times that did wrong " + error);
        }

        // sketch.toString2();
    }

    public static final class CMmapFunction implements MapFunction<String, Tuple2<Integer,Integer>> {

        public Tuple2<Integer,Integer> map(String value) {
            // normalize and split the line
            String[] tokens = value.split("\n");
            int arg1=0;
            int arg2=0;
            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    String[] ar = token.split(",");
                    arg1 = Integer.parseInt(ar[0]);
                    arg2 = Integer.parseInt(ar[1]);

                    sketch.add(arg1, arg2); //insert the data to the hash table

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
            // normalize and split the line
            String[] tokens = value.split("\n");
            int arg1=0;
            int arg2=0;

            // emit the pairs
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
            // normalize and split the line
            String[] tokens = value.split("\n");
            String arg1 = null;
            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    arg1 = token;

                    if (freq.containsKey(arg1)) {   //compute the actual data
                        freq.put(Integer.parseInt(arg1), freq.get(arg1) + 1);
                    } else {
                        freq.put(Integer.parseInt(arg1), 1);
                    }

                    bf.add(arg1);   //add the element to the hash table
                }
            }
            return new Tuple1(arg1);

        }
    }

    public static final class BFQMapFunction implements MapFunction<String, Tuple2<String, Boolean>> {

        public Tuple2<String, Boolean> map(String value) {
            // normalize and split the line
            String[] tokens = value.split("\n");
            String arg1 = null;
            boolean arg2 = false;
            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    arg1 = token;
                    qcount++;
                    arg2 = bf.contains(arg1);   //check if the element exist
                    qcount++;
                    if (arg2) error++;
                }
            }
            return new Tuple2(arg1,arg2);
        }
    }

}

