package count_min.count_min;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.*;


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


		/*final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served every second */

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		DataStream<String> text = env.readTextFile(params.get("input","/home/dimitra/dataBig.txt"));
		
		DataStream<Tuple2<String, Integer>> counts =
				// split up the lines in pairs (2-tuples) containing: (word,1)
				text.flatMap(new Tokenizer())
				// group by the tuple field "0" and sum up tuple field "1"
				.keyBy(0); //.sum(1);
				
			// emit result
				counts.writeAsText(params.get("output","/home/dimitra/input.txt"), FileSystem.WriteMode.OVERWRITE).setParallelism(1);


			Count_Min sketch = new Count_Min(100,100,30);   //experimentally

		try {
			File file = new File("/home/dimitra/input.txt");

			BufferedReader br = new BufferedReader(new FileReader(file));
	        String str;
	        int arg1, arg2;
	        while ((str = br.readLine()) != null) {
				String replaced = str.replaceAll("[()]", "");
	            String[] ar = replaced.split(",");
	            arg1 = Integer.parseInt(ar[0].trim());
	            arg2 = Integer.parseInt(ar[1].trim());

	            sketch.add(arg1,arg2);

	        }
	    } catch (IOException e) {
	        System.out.println("File Read Error");
	    }


        File file1 = new File("/home/dimitra/queries.txt");
        String str1;
        int arg3;
        long est;
        BufferedReader br1 = new BufferedReader(new FileReader(file1));
        File statText = new File("/home/dimitra/estimations.txt");
        FileOutputStream is = new FileOutputStream(statText);
        OutputStreamWriter osw = new OutputStreamWriter(is);
        Writer w = new BufferedWriter(osw);

        while ((str1 = br1.readLine()) != null) {
            String[] ar1 = str1.split("\n");
            arg3 = Integer.parseInt(ar1[0]);

            est= sketch.estimateCount(arg3);
            String strLong = Long.toString(est);
            //System.out.println("str long is " + strLong);
            w.write("This is the estimation of "+arg3 + ": " + strLong + "\n");

        }
        w.close();


        // execute program
        env.execute("Streaming Count_Min");
	}

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

