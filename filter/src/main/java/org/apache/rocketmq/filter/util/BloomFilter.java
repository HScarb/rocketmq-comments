/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.filter.util;

import com.google.common.hash.Hashing;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Simple implement of bloom filter.
 */
public class BloomFilter {

    public static final Charset UTF_8 = StandardCharsets.UTF_8;

    // 最大错误率
    // as error rate, 10/100 = 0.1
    private int f = 10;
    // 可能插入的元素个数
    private int n = 128;

    // 哈希函数个数
    // hash function num, by calculation.
    private int k;
    // bit 数组位数
    // bit count, by calculation.
    private int m;

    /**
     * Create bloom filter by error rate and mapping num.
     *
     * @param f error rate
     * @param n num will mapping to bit
     */
    public static BloomFilter createByFn(int f, int n) {
        return new BloomFilter(f, n);
    }

    /**
     * Constructor.
     *
     * @param f error rate
     * @param n num will mapping to bit
     */
    private BloomFilter(int f, int n) {
        if (f < 1 || f >= 100) {
            throw new IllegalArgumentException("f must be greater or equal than 1 and less than 100");
        }
        if (n < 1) {
            throw new IllegalArgumentException("n must be greater than 0");
        }

        this.f = f;
        this.n = n;

        // set p = e^(-kn/m)
        // f = (1 - p)^k = e^(kln(1-p))
        // when p = 0.5, k = ln2 * (m/n), f = (1/2)^k = (0.618)^(m/n)
        double errorRate = f / 100.0;
        this.k = (int) Math.ceil(logMN(0.5, errorRate));

        if (this.k < 1) {
            throw new IllegalArgumentException("Hash function num is less than 1, maybe you should change the value of error rate or bit num!");
        }

        // m >= n*log2(1/f)*log2(e)
        this.m = (int) Math.ceil(this.n * logMN(2, 1 / errorRate) * logMN(2, Math.E));
        // m%8 = 0
        this.m = (int) (Byte.SIZE * Math.ceil(this.m / (Byte.SIZE * 1.0)));
    }

    /**
     * 计算给定字符串的哈希值，进行 k 次哈希，返回一个 int 数组，数组中的每个值是位的位置。
     * Calculate bit positions of {@code str}.
     * <p>
     * See "Less Hashing, Same Performance: Building a Better Bloom Filter" by Adam Kirsch and Michael
     * Mitzenmacher.
     * </p>
     */
    public int[] calcBitPositions(String str) {
        int[] bitPositions = new int[this.k];

        long hash64 = Hashing.murmur3_128().hashString(str, UTF_8).asLong();

        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);

        for (int i = 1; i <= this.k; i++) {
            int combinedHash = hash1 + (i * hash2);
            // Flip all the bits if it's negative (guaranteed positive number)
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            bitPositions[i - 1] = combinedHash % this.m;
        }

        return bitPositions;
    }

    /**
     * 接收一个字符串 str，计算它在Bloom过滤器中对应的位的位置，然后返回一个 BloomFilterData 对象，该对象包含这些位置。
     * Calculate bit positions of {@code str} to construct {@code BloomFilterData}
     */
    public BloomFilterData generate(String str) {
        int[] bitPositions = calcBitPositions(str);

        return new BloomFilterData(bitPositions, this.m);
    }

    /**
     * 接收一个字符串 str 和一个 BitsArray 对象，首先计算字符串对应的位的位置，然后在 BitsArray 对象中将这些位置设置为1。
     * Calculate bit positions of {@code str}, then set the related {@code bits} positions to 1.
     */
    public void hashTo(String str, BitsArray bits) {
        hashTo(calcBitPositions(str), bits);
    }

    /**
     * 接收一个整数数组和一个 BitsArray 对象，整数数组表示位的位置，该方法将 BitsArray 对象中对应位置的位设置为1。
     * Set the related {@code bits} positions to 1.
     */
    public void hashTo(int[] bitPositions, BitsArray bits) {
        check(bits);

        for (int i : bitPositions) {
            bits.setBit(i, true);
        }
    }

    /**
     * 接收一个 BloomFilterData 对象和一个 BitsArray 对象，将 BitsArray 对象中对应位置的位设置为 1。
     * Extra check:
     * <li>1. check {@code filterData} belong to this bloom filter.</li>
     * <p>
     * Then set the related {@code bits} positions to 1.
     * </p>
     */
    public void hashTo(BloomFilterData filterData, BitsArray bits) {
        if (!isValid(filterData)) {
            throw new IllegalArgumentException(
                String.format("Bloom filter data may not belong to this filter! %s, %s",
                    filterData, this)
            );
        }
        hashTo(filterData.getBitPos(), bits);
    }

    /**
     * 接收一个字符串和一个 BitsArray 对象，检查给定的字符串是否可能存在于Bloom过滤器所表示的集合中。
     * Calculate bit positions of {@code str}, then check all the related {@code bits} positions is 1.
     *
     * @return true: all the related {@code bits} positions is 1
     */
    public boolean isHit(String str, BitsArray bits) {
        return isHit(calcBitPositions(str), bits);
    }

    /**
     * 检查给定的位位置是否都已经被设置为 1。
     * Check all the related {@code bits} positions is 1.
     *
     * @return true: all the related {@code bits} positions is 1
     */
    public boolean isHit(int[] bitPositions, BitsArray bits) {
        check(bits);
        boolean ret = bits.getBit(bitPositions[0]);
        for (int i = 1; i < bitPositions.length; i++) {
            ret &= bits.getBit(bitPositions[i]);
        }
        return ret;
    }

    /**
     * 检查给定的 BloomFilterData 对应的位位置是否都已经被设置为 1。
     * Check all the related {@code bits} positions is 1.
     *
     * @return true: all the related {@code bits} positions is 1
     */
    public boolean isHit(BloomFilterData filterData, BitsArray bits) {
        if (!isValid(filterData)) {
            throw new IllegalArgumentException(
                String.format("Bloom filter data may not belong to this filter! %s, %s",
                    filterData, this)
            );
        }
        return isHit(filterData.getBitPos(), bits);
    }

    /**
     * Check whether one of {@code bitPositions} has been occupied.
     *
     * @return true: if all positions have been occupied.
     */
    public boolean checkFalseHit(int[] bitPositions, BitsArray bits) {
        for (int j = 0; j < bitPositions.length; j++) {
            int pos = bitPositions[j];

            // check position of bits has been set.
            // that mean no one occupy the position.
            if (!bits.getBit(pos)) {
                return false;
            }
        }

        return true;
    }

    protected void check(BitsArray bits) {
        if (bits.bitLength() != this.m) {
            throw new IllegalArgumentException(
                String.format("Length(%d) of bits in BitsArray is not equal to %d!", bits.bitLength(), this.m)
            );
        }
    }

    /**
     * Check {@code BloomFilterData} is valid, and belong to this bloom filter.
     * <li>1. not null</li>
     * <li>2. {@link org.apache.rocketmq.filter.util.BloomFilterData#getBitNum} must be equal to {@code m} </li>
     * <li>3. {@link org.apache.rocketmq.filter.util.BloomFilterData#getBitPos} is not null</li>
     * <li>4. {@link org.apache.rocketmq.filter.util.BloomFilterData#getBitPos}'s length is equal to {@code k}</li>
     */
    public boolean isValid(BloomFilterData filterData) {
        if (filterData == null
            || filterData.getBitNum() != this.m
            || filterData.getBitPos() == null
            || filterData.getBitPos().length != this.k) {
            return false;
        }

        return true;
    }

    /**
     * error rate.
     */
    public int getF() {
        return f;
    }

    /**
     * expect mapping num.
     */
    public int getN() {
        return n;
    }

    /**
     * hash function num.
     */
    public int getK() {
        return k;
    }

    /**
     * total bit num.
     */
    public int getM() {
        return m;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof BloomFilter))
            return false;

        BloomFilter that = (BloomFilter) o;

        if (f != that.f)
            return false;
        if (k != that.k)
            return false;
        if (m != that.m)
            return false;
        if (n != that.n)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = f;
        result = 31 * result + n;
        result = 31 * result + k;
        result = 31 * result + m;
        return result;
    }

    @Override
    public String toString() {
        return String.format("f: %d, n: %d, k: %d, m: %d", f, n, k, m);
    }

    protected double logMN(double m, double n) {
        return Math.log(n) / Math.log(m);
    }
}
