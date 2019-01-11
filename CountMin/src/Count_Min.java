/**
 * 
 */

/**
 * @author igkouzionis
 *
 */

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Random;

public class Count_Min {
	//to minimize collisions it is important to reduce the number of common factors between m and the elements of K. 
		//By choosing m to be a number that has very few factors: a prime number.
			final long PRIME_MODULUS = (1L << 31) - 1; 

		    int depth;
		    int width;
		    long[][] countArray;
		    long[] hashA;
		    long size;
		    double eps;
		    double confidence;
			
		    public Count_Min(int depth, int width, int seed) {
		        this.depth = depth;
		        this.width = width;
		        this.eps = 2.0 / width;
		        this.confidence = 1 - 1 / Math.pow(2, depth);
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
		    
		    public Count_Min(double epsOfTotalCount, double confidence, int seed) {
		        // 1/2^depth <= 1-confidence ; depth >= -log2 (1-confidence)
		        this.eps = epsOfTotalCount;
		        this.confidence = confidence;
		        this.width = (int) Math.ceil(2 / epsOfTotalCount);
		        this.depth = (int) Math.ceil(-Math.log(1 - confidence) / Math.log(2));
		        initCountArray(depth, width, seed);
		    }

		    Count_Min(int depth, int width, long size, long[] hashA, long[][] countArray) {
		        this.depth = depth;
		        this.width = width;
		        this.eps = 2.0 / width;
		        this.confidence = 1 - 1 / Math.pow(2, depth);
		        this.hashA = hashA;
		        this.countArray = countArray;
		        this.size = size;
		    }
		    
		    public Count_Min() {
				// TODO Auto-generated constructor stub
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
		    
		    public double getRelativeError() {
		        return eps;
		    }

		    public double getConfidence() {
		        return confidence;
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
		    			
		    public void add(long item, long count) {
		        if (count < 0) {
		            throw new IllegalArgumentException("Negative increments not implemented");
		        }
		        for (int i = 0; i < depth; ++i) {
		            countArray[i][hash(item, i)] += count;
		        }

		    }

		    public void add(String item, long count) {
		        if (count < 0) {
		            throw new IllegalArgumentException("Negative increments not implemented");
		        }
		        int[] buckets = getHashBuckets(item, depth, width);
		        for (int i = 0; i < depth; ++i) {
		            countArray[i][buckets[i]] += count;
		        }

		    }

		    /**
		     * The estimate is correct within 'epsilon' * (total item count),
		     * with probability 'confidence'.
		     */
		    public long estimateCount(long item) {
		        long res = Long.MAX_VALUE;
		        for (int i = 0; i < depth; ++i) {
		            res = Math.min(res, countArray[i][hash(item, i)]);
		        }
		        return res;
		    }

		    public long estimateCount(String item) {
		        long res = Long.MAX_VALUE;
		        int[] buckets = getHashBuckets(item, depth, width);
		        for (int i = 0; i < depth; ++i) {
		            res = Math.min(res, countArray[i][buckets[i]]);
		        }
		        return res;
		    }
		    
		    
		    // Murmur is faster than an SHA-based approach and provides as-good collision
		    // resistance.  The combinatorial generation approach described in
		    // http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf
		    // does prove to work in actual tests, and is obviously faster
		    // than performing further iterations of murmur.
		    public static int[] getHashBuckets(String key, int hashCount, int max) {
		        byte[] b;
		        try {
		            b = key.getBytes("UTF-16");
		        } catch (UnsupportedEncodingException e) {
		            throw new RuntimeException(e);
		        }
		        return getHashBuckets(b, hashCount, max);
		    }

		    static int[] getHashBuckets(byte[] b, int hashCount, int max) {
		        int[] result = new int[hashCount];
		        int hash1 = MurmurHash.hash(b, b.length, 0);
		        int hash2 = MurmurHash.hash(b, b.length, hash1);
		        for (int i = 0; i < hashCount; i++) {
		            result[i] = Math.abs((hash1 + i * hash2) % max);
		        }
		        return result;
		    }
		    
		    
		    public static byte[] serialize(Count_Min sketch) {
		        ByteArrayOutputStream bos = new ByteArrayOutputStream();
		        DataOutputStream s = new DataOutputStream(bos);
		        try {
		            s.writeLong(sketch.size);
		            s.writeInt(sketch.depth);
		            s.writeInt(sketch.width);
		            for (int i = 0; i < sketch.depth; ++i) {
		                s.writeLong(sketch.hashA[i]);
		                for (int j = 0; j < sketch.width; ++j) {
		                    s.writeLong(sketch.countArray[i][j]);
		                }
		            }
		            return bos.toByteArray();
		        } catch (IOException e) {
		            throw new RuntimeException(e);
		        }
		    }

		    public static Count_Min deserialize(byte[] data) {
		        ByteArrayInputStream bis = new ByteArrayInputStream(data);
		        DataInputStream s = new DataInputStream(bis);
		        try {
		            Count_Min sketch = new Count_Min();
		            sketch.size = s.readLong();
		            sketch.depth = s.readInt();
		            sketch.width = s.readInt();
		            sketch.eps = 2.0 / sketch.width;
		            sketch.confidence = 1 - 1 / Math.pow(2, sketch.depth);
		            sketch.hashA = new long[sketch.depth];
		            sketch.countArray = new long[sketch.depth][sketch.width];
		            for (int i = 0; i < sketch.depth; ++i) {
		                sketch.hashA[i] = s.readLong();
		                for (int j = 0; j < sketch.width; ++j) {
		                    sketch.countArray[i][j] = s.readLong();
		                }
		            }
		            return sketch;
		        } catch (IOException e) {
		            throw new RuntimeException(e);
		        }
		    }

		    
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
		    
		    /**
		     * Merge the give count min sketch with current one. Merge will throw RuntimeException if the
		     * provided CountMinSketch is not compatible with current one.
		     *
		     * @param that - the one to be merged
		     */
		    public void merge(Count_Min that) {
		      if (that == null) {
		        return;
		      }

		      if (this.width != that.width) {
		        throw new RuntimeException("Merge failed! Width of count min sketch do not match!" +
		            "this.width: " + this.getWidth() + " that.width: " + that.getWidth());
		      }

		      if (this.depth != that.depth) {
		        throw new RuntimeException("Merge failed! Depth of count min sketch do not match!" +
		            "this.depth: " + this.getDepth() + " that.depth: " + that.getDepth());
		      }

		      for (int i = 0; i < depth; i++) {
		        for (int j = 0; j < width; j++) {
		          this.countArray[i][j] += that.countArray[i][j];
		        }
		      }
		    }

			public int getDepth() {
				return depth;
			}

			public void setDepth(int depth) {
				this.depth = depth;
			}

			public int getWidth() {
				return width;
			}

			public void setWidth(int width) {
				this.width = width;
			}
}
