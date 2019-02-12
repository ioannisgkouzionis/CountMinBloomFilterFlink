package count_min.count_min;

import java.util.Arrays;
import java.util.Random;

public class Count_Min {
	private final long PRIME_MODULUS = (1L << 31) - 1;
	private int depth;
	private int width;
	private long[][] countArray;
	private long[] hashnum;

	/*The data structure accepts two parameters epsilon and confidence, epsilon specifies
 	the error in estimation and confidence specifies the probability that the estimation is correct */

	private double eps;
	private double confidence;

	//not used
	public Count_Min(int depth, int width, int seed) {
		this.depth = depth;
		this.width = width;
		this.eps = 2.0 / width;
		this.confidence = 1 - 1 / Math.pow(2, depth);
		initCountArray(depth, width, seed);
	}

	private void initCountArray(int depth, int width, int seed) {
		this.countArray = new long[depth][width];
		this.hashnum = new long[depth];
		Random r = new Random(seed);
		for (int i = 0; i < depth; ++i) {
			hashnum[i] = r.nextInt(Integer.MAX_VALUE);	//linear hash functions of the form (a*x+b) mod p. a,b are chosen independently for each hash function.
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

	public Count_Min() {

	}

	//print the context
	public String toString() {
		return "CountMinSketch{" +
				"eps=" + eps +
				", confidence=" + confidence +
				", depth=" + depth +
				", width=" + width +
				'}';
	}

	private int hash(long item, int i) {
		long hash = hashnum[i] * item;
		hash += hash >> 32;
		hash &= PRIME_MODULUS;
		return ((int) hash) % width;
	}

	void add(long item, long count) {
		for (int i = 0; i < depth; ++i) {
			countArray[i][hash(item, i)] += count;
		}

	}


	//function for insert string items, not used
//		    public void add(String item, long count) {
//		        int[] buckets = getHashBuckets(item, depth, width);
//		        for (int i = 0; i < depth; ++i) {
//		            countArray[i][buckets[i]] += count;
//
//		        }
//
//		    }

	//compute the estimation
	long estimateCount(long item) {
		long res = Long.MAX_VALUE;
		for (int i = 0; i < depth; ++i) {
			res = Math.min(res, countArray[i][hash(item, i)]);
		}
		return res;
	}

	//function to compute estimation of string items, not used
//	public long estimateCount(String item) {
//		long res = Long.MAX_VALUE;
//		int[] buckets = getHashBuckets(item, depth, width);
//		for (int i = 0; i < depth; ++i) {
//			res = Math.min(res, countArray[i][buckets[i]]);
//		}
//		return res;
//	}



	//hashfunction for string items, not used
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

		if (!Arrays.deepEquals(countArray, that.countArray)) {
			return false;
		}
		return Arrays.equals(hashnum, that.hashnum);
	}

}