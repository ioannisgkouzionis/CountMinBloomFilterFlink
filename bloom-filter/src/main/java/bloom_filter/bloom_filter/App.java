package bloom_filter.bloom_filter;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class App {
	
	static int elementCount = 100; // Number of elements to test
	
	public static void printStat(long start, long end) {
        double diff = (end - start) / 1000.0;
        System.out.println(diff + "s, " + (elementCount / diff) + " elements/s");
    }

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		final ParameterTool params= ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		DataStream<String> text = env.readTextFile(params.get("input","C:/Users/igkouzionis/Documents/GitHub/CountMinFlink/TxtFiles/datatest.txt")).setParallelism(1);
		Iterator<String> myOutput = DataStreamUtils.collect(text);
		
		// create list
		List<String> list = new ArrayList(elementCount);
		
		System.out.println("Testing " + elementCount + " elements");
		
		while (myOutput.hasNext())
		    list.add(myOutput.next());
        
        BloomFilter<String> bf = new BloomFilter<String>(0.001, elementCount);
        
        System.out.println("Testing " + elementCount + " elements");
        System.out.println("k is " + bf.getK()); // how?
        
//        Add elements
        System.out.print("add(): ");
        long start_add = System.currentTimeMillis();
        for (int i = 0; i < elementCount; i++) {
            bf.add(list.get(i));
        }
        long end_add = System.currentTimeMillis();
        printStat(start_add, end_add);
        
        File file1 = new File("C:/Users/igkouzionis/Documents/GitHub/CountMinFlink/TxtFiles/queries.txt");
        
        String str1;
        String arg3;
        boolean est;
        BufferedReader br1 = new BufferedReader(new FileReader(file1));
        File statText = new File("C:/Users/igkouzionis/Documents/GitHub/CountMinFlink/TxtFiles/contains.txt");
        FileOutputStream is = new FileOutputStream(statText);
        OutputStreamWriter osw = new OutputStreamWriter(is);
        Writer w = new BufferedWriter(osw);

        while ((str1 = br1.readLine()) != null) {
            String[] ar1 = str1.split("\n");
            arg3 = ar1[0];
            est = bf.contains(arg3);
            w.write("Element " + arg3 + "is included? : " + est + "\n");

        }
        w.close();
        
//        execute program
        env.execute("Streaming Bloom_Filter");
	}

}

