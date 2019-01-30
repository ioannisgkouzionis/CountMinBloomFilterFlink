package count_min.count_min;


import org.joda.time.DateTime;
import java.util.Arrays;
import java.util.Random;
import java.io.Serializable;

public class Count_Min implements Comparable<Count_Min>, Serializable {


	private final long PRIME_MODULUS = (1L << 31) - 1;

	private int depth;
	private int width;
	private long[][] countArray;
	private long[] hashA;
	private long size;
	long item,count;

	private boolean isStart=true;
//	public DateTime startTime;
//	public DateTime endTime;


	/*The data structure accepts two parameters epsilon and confidence, epsilon specifies
 	the error in estimation and confidence specifies the probability that the estimation is correct */

	private double eps;
	private double confidence;


	public Count_Min(int depth, int width, int seed) {
		this.depth = depth;
		this.width = width;
		this.eps = 2.0 / width;			//0.02
		this.confidence = 1 - 1 / Math.pow(2, depth);		//1
		initCountArray(depth, width, seed);
	}

	private void initCountArray(int depth, int width, int seed) {
		this.countArray = new long[depth][width];
		this.hashA = new long[depth];
		Random r = new Random(seed);
		for (int i = 0; i < depth; ++i) {
			hashA[i] = r.nextInt(Integer.MAX_VALUE);	//linear hash functions of the form (a*x+b) mod p. a,b are chosen independently for each hash function.
		}
	}

	Count_Min(double eps, double confidence, int seed) {

		this.eps = eps;
		this.confidence = confidence;
		this.width = (int) Math.ceil(2 / eps);
		this.depth = (int) Math.ceil(-Math.log(1 - confidence) / Math.log(2));
        System.out.println("The depth is: " + depth);
        System.out.println("The width is: " + width);
		initCountArray(depth, width, seed);
	}



	private Count_Min() {

	}

	public String toString() {
		return "CountMinSketch{" +
				"eps=" + eps +
				", confidence=" + confidence +
				", depth=" + depth +
				", width=" + width +
				", size=" + size +
				'}';
	}



	public void toString2() {
		for (int i = 0; i < depth; i++) {
			for (int j = 0; j < width; j++) {
				System.out.print(countArray[i][j] + "||") ;
			}
			System.out.print("\n");
		}
	}

	int hash(long item, int i) {
		long hash = hashA[i] * item;
		// A super fast way of computing x mod 2^p-1
		// See http://www.cs.princeton.edu/courses/archive/fall09/cos521/Handouts/universalclasses.pdf
		// page 149, right after Proposition 7.
		hash += hash >> 32;
		hash &= PRIME_MODULUS;
		// Doing "%" after (int) conversion is ~2x faster than %'ing longs.
		return ((int) hash) % width;
	}

	void add(long item, long count) {

		for (int i = 0; i < depth; ++i) {
			countArray[i][hash(item, i)] += count;
			//System.out.println("Item is " + item + " this is countarray: " + countArray[i][hash(item, i)] +"me count"+ count);
		}

	}

//		    public void add(String item, long count) {
//		        if (count < 0) {
//		            throw new IllegalArgumentException("Negative increments not implemented");
//		        }
//		        int[] buckets = getHashBuckets(item, depth, width);
//		        for (int i = 0; i < depth; ++i) {
//		            countArray[i][buckets[i]] += count;
//					System.out.println("Item is " + item + " this is countarray: " + countArray[i][buckets[i]] +"me count"+ count);
//		        }
//
//		    }

	long estimateCount(long item) {
		long res = Long.MAX_VALUE;
		// System.out.println("This is res " + res );
		for (int i = 0; i < depth; ++i) {
			res = Math.min(res, countArray[i][hash(item, i)]);
			//System.out.println("this is countarray: " + countArray[i][hash(item, i)]+ "with res " + res);
		}
		return res;
	}

//	public long estimateCount(String item) {
//		long res = Long.MAX_VALUE;
//		int[] buckets = getHashBuckets(item, depth, width);
//		for (int i = 0; i < depth; ++i) {
//			res = Math.min(res, countArray[i][buckets[i]]);
//		}
//		return res;
//	}


//	public static int[] getHashBuckets(String key, int hashCount, int max) {
//		byte[] b;
//		try {
//			b = key.getBytes("UTF-8");
//		} catch (UnsupportedEncodingException e) {
//			throw new RuntimeException(e);
//		}
//		return getHashBuckets(b, hashCount, max);
//	}

//	static int[] getHashBuckets(byte[] b, int hashCount, int max) {
//		int[] result = new int[hashCount];
//		int hash1 = MurmurHash.hash(b, b.length, 0);
//		int hash2 = MurmurHash.hash(b, b.length, hash1);
//		for (int i = 0; i < hashCount; i++) {
//			result[i] = Math.abs((hash1 + i * hash2) % max);
//		}
//		return result;
//	}



	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		final Count_Min that = (Count_Min) o;

		if (depth != that.depth) {
			return false;
		}
		if (width != that.width) {
			return false;
		}

		if (Double.compare(that.eps, eps) != 0) {
			return false;
		}
		if (Double.compare(that.confidence, confidence) != 0) {
			return false;
		}

		if (size != that.size) {
			return false;
		}

		if (!Arrays.deepEquals(countArray, that.countArray)) {
			return false;
		}
		return Arrays.equals(hashA, that.hashA);
	}



	static Count_Min fromString(String line) {

		String[] tokens = line.split(",");

		Count_Min cm = new Count_Min();

		try {
			cm.item = Long.parseLong(tokens[0]);
			cm.count = Long.parseLong(tokens[1]);


		} catch (NumberFormatException nfe) {
			throw new RuntimeException("Invalid record: " + line, nfe);
		}

		return cm;
	}


	//////////////////////????????????????????????????????????????????////////////////////////////

	long getEventTime() {
		return 1;
//        if (isStart) {
//            return 1;
//        }
//        else {
//
//            return 2;
//        }
	}




	@Override
	public int compareTo(Count_Min other) {
		if (other == null) {
			return 1;
		}
		int compareTimes = Long.compare(this.getEventTime(), other.getEventTime());
		if (compareTimes == 0) {
			if (this.isStart == other.isStart) {
				return 0;
			}
			else {
				if (this.isStart) {
					return -1;
				}
				else {
					return 1;
				}
			}
		}
		else {
			return compareTimes;
		}
	}
}