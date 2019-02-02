package count_min.count_min;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;


public class BloomFilter<E> implements Serializable {
    private BitSet bitset;
    private int bitSetSize;
    private double bitsPerElement;
    private int expectedNumberOfFilterElements; // expected (maximum) number of elements to be added
    private int numberOfAddedElements; // number of elements actually added to the Bloom filter
    private int k; // number of hash functions

    private static final Charset charset = Charset.forName("UTF-8"); // encoding used for storing hash values as strings

    //An MD5 hash is created by taking a string of an any length and encoding it into a 128-bit fingerprint
    private static final String hashName = "MD5";

    //Message digests are secure one-way hash functions that take arbitrary-sized data and output a fixed-length hash value.
    private static final MessageDigest digestFunction;
    static {
        MessageDigest tmp;
        try {
            tmp = java.security.MessageDigest.getInstance(hashName);
        } catch (NoSuchAlgorithmException e) {
            tmp = null;
        }
        digestFunction = tmp;
    }

    BloomFilter(double falsePositiveProbability, int expectedNumberOfElements) {

        this.expectedNumberOfFilterElements = expectedNumberOfElements;
        this.k = (int)Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2))); // k = ceil(-log_2(false prob.));
        this.bitsPerElement = Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2))) / Math.log(2); // c = k / ln(2)
        this.bitSetSize = (int)Math.ceil(Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2))) / Math.log(2) * expectedNumberOfElements);
        numberOfAddedElements = 0;
        this.bitset = new BitSet(bitSetSize);
    }


    //Generates a digest based on the contents of a String.
    private static int createHash(String val, Charset charset) {
        return createHash(val.getBytes(charset));
    }

    public static int createHash(String val) {
        return createHash(val, charset);
    }

    private static int createHash(byte[] data) {
        return createHashes(data, 1)[0];
    }

    private static int[] createHashes(byte[] data, int hashes) {
        int[] result = new int[hashes];

        int k = 0;
        byte salt = 0;
        while (k < hashes) {
            byte[] digest;
            synchronized (digestFunction) {
                digestFunction.update(salt);
                salt++;
                digest = digestFunction.digest(data);
            }

            for (int i = 0; i < digest.length/4 && k < hashes; i++) {
                int h = 0;
                for (int j = (i*4); j < (i*4)+4; j++) {
                    h <<= 8;
                    h |= ((int) digest[j]) & 0xFF;
                }
                result[k] = h;
                k++;
            }
        }
        return result;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 61 * hash + (this.bitset != null ? this.bitset.hashCode() : 0);
        hash = 61 * hash + this.expectedNumberOfFilterElements;
        hash = 61 * hash + this.bitSetSize;
        hash = 61 * hash + this.k;
        return hash;
    }



    // Calculates the expected probability of false positives based
    // on the number of expected filter elements and the size of the Bloom filter.

    double expectedFalsePositiveProbability() {
        return getFalsePositiveProbability(expectedNumberOfFilterElements);
    }

    private double getFalsePositiveProbability(double numberOfElements) {
        // (1 - e^(-k * n / m)) ^ k
        return Math.pow((1 - Math.exp(-k * (double) numberOfElements
                / (double) bitSetSize)), k);

    }

    //Adds an element to the Bloom filter

    void add(E element) {
        add(element.toString().getBytes(charset));
    }

    private void add(byte[] bytes) {
        int[] hashes = createHashes(bytes, k);
        for (int hash : hashes)
            bitset.set(Math.abs(hash % bitSetSize), true);
        numberOfAddedElements ++;
    }


    // true if the element could have been inserted into the Bloom filter.
    boolean contains(E element) {
        return contains(element.toString().getBytes(charset));
    }

    private boolean contains(byte[] bytes) {
        int[] hashes = createHashes(bytes, k);
        for (int hash : hashes) {
            if (!bitset.get(Math.abs(hash % bitSetSize))) {
                return false;
            }
        }
        return true;
    }

    double getBitsPerElement() {
        return this.bitSetSize / (double)numberOfAddedElements;
    }
}
