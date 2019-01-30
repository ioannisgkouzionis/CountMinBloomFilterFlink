package bloom_filter.bloom_filter;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class App {
	
	static int elementCount = 1000000; // Number of elements to test
	
//	public static void printStat(long start, long end) {
//        double diff = (end - start) / 1000.0;
//        System.out.println(diff + "s, " + (elementCount / diff) + " elements/s");
//    }

    public static BloomFilter<String> bf = new BloomFilter<String>(0.0001, elementCount);

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		final ParameterTool params= ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.getConfig().setGlobalJobParameters(params);
        
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Tuple1<String>> text = env.readTextFile("Ndata.txt").map(new MyMapFunction());

//		Iterator<String> myOutput = DataStreamUtils.collect(text);
		
		// create list
//		List<String> list = new ArrayList(elementCount);
//
//		System.out.println("Testing " + elementCount + " elements");
//
//		while (myOutput.hasNext())
//		    list.add(myOutput.next());
        
//        BloomFilter<String> bf = new BloomFilter<String>(0.001, elementCount);
//
//        System.out.println("Testing " + elementCount + " elements");
//        System.out.println("k is " + bf.getK()); // how?
        
//        Add elements
//        System.out.print("add(): ");
//        long start_add = System.currentTimeMillis();
//        for (int i = 0; i < elementCount; i++) {
//            bf.add(list.get(i));
//        }
//        long end_add = System.currentTimeMillis();
//        printStat(start_add, end_add);

        DataStream<Tuple2<String, Boolean>> text2 =env.readTextFile("Queries.txt").map(new MyOtherMapFunction()).keyBy(0);

        text2.writeAsText("output_bf.txt", FileSystem.WriteMode.OVERWRITE);
        
//        File file1 = new File("C:/Users/igkouzionis/Documents/GitHub/CountMinFlink/TxtFiles/queries.txt");
//
//        String str1;
//        String arg3;
//        boolean est;
//        BufferedReader br1 = new BufferedReader(new FileReader(file1));
//        File statText = new File("C:/Users/igkouzionis/Documents/GitHub/CountMinFlink/TxtFiles/contains.txt");
//        FileOutputStream is = new FileOutputStream(statText);
//        OutputStreamWriter osw = new OutputStreamWriter(is);
//        Writer w = new BufferedWriter(osw);
//
//        while ((str1 = br1.readLine()) != null) {
//            String[] ar1 = str1.split("\n");
//            arg3 = ar1[0];
//            est = bf.contains(arg3);
//            w.write("Element " + arg3 + "is included? : " + est + "\n");
//
//        }
//        w.close();
        
//        execute program
        JobExecutionResult result = env.execute("Streaming Bloom_Filter");
        System.out.println("The job took " + result.getNetRuntime(TimeUnit.MILLISECONDS) + " milliseconds to execute");
	}

    public static final class MyMapFunction implements MapFunction<String, Tuple1<String>> {

        public Tuple1<String> map(String value) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\n");
            String arg1 = null;
            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    String ar = token;
                    arg1 = ar;
                    bf.add(arg1);
                }
            }
            return new Tuple1(arg1);

        }
    }

    public static final class MyOtherMapFunction implements MapFunction<String, Tuple2<String, Boolean>> {

        public Tuple2<String, Boolean> map(String value) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\n");
            String arg1 = null;
            boolean arg2 = false;
            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    // String replaced = token.replaceAll("[()]", "");
                    String ar = token;
                    arg1 = ar;
                    arg2 = bf.contains(arg1);

                }
            }
            return new Tuple2(arg1,arg2);

        }
    }

}

