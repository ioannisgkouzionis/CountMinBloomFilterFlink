import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * 
 */

/**
 * @author igkouzionis
 *
 */

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class My_Main {

	/**
	 * @param args
	 * @throws Exception 
	 */
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		final ParameterTool params= ParameterTool.fromArgs(args);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(params);
		
		//DataStream<String> stream = env.readTextFile("data.txt");
		
		DataStream<String> text = null;
		if(params.has("data2")) {
			text = env.readTextFile(params.get("data2"));
		} else {
			System.out.println("Use default dataset");
		}
		
		DataStream<Tuple2<String, Integer>> counts =
				// split up the lines in pairs (2-tuples) containing: (word,1)
				text.flatMap(new Tokenizer())
				// group by the tuple field "0" and sum up tuple field "1"
				.keyBy(0); // keyBy(0).sum(1);
				
			// emit result
			if (params.has("output")) {
				counts.writeAsText(params.get("output"));
			} else {
				System.out.println("Printing result to stdout. Use --output to specify output path.");
				counts.print();
			}
			
			

			// execute program
			env.execute();
			
	}
	
	
			
	/**
	 * Implements the string tokenizer that splits sentences into words as a
	 * user-defined FlatMapFunction. The function takes a line (String) and
	 * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
	 * Integer>}).
	 */
	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\n");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1));
				}
			}
		}
	}
			
		
		
		

		/*Count_Min s = new Count_Min(10,10,30);
		
		try {
	        BufferedReader in = new BufferedReader(new FileReader("data2.txt"));
	        String str;
	        int arg1, arg2;
	        while ((str = in.readLine()) != null) {
	            String[] ar = str.split(",");
	            arg1 = Integer.parseInt(ar[0]);
	            arg2 = Integer.parseInt(ar[1]);
	            s.add(arg1,arg2);
	        }
	        in.close();
	    } catch (IOException e) {
	        System.out.println("File Read Error");
	    }
		System.out.println(s.estimateCount(17));*/
		
		
		// Count_Min s = new Count_Min(10,10,30);
		//s.add(arg1, arg2);
		
        /*Count_Min s = new Count_Min(10,10,30);
        s.add(20,1);
        s.add(10,3);
        s.add(30,1);
        s.add(10,1);
        
        Count_Min t = new Count_Min(10,10,30);
        t.add(10, 2);
        t.add("hello", 1);
        
        // t.merge(s);*/        
        // System.out.println(s.estimateCount(10));
	}


