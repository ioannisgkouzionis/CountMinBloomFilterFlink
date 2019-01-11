package count_min.count_min;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;



/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception
    {
    	final ParameterTool params= ParameterTool.fromArgs(args);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(params);
		
		//DataStream<String> stream = env.readTextFile("data.txt");
		
		DataStream<String> text = null;
		if(params.has("input")) {
			text = env.readTextFile(params.get("input"));
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
			env.execute("Streaming Count_Min");
	}
	
	
			
	/**
	 * Implements the string tokenizer that splits sentences into words as a
	 * user-defined FlatMapFunction. The function takes a line (String) and
	 * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
	 * Integer>}).
	 */
	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\n");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}
}

