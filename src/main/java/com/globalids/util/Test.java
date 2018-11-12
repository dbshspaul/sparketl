package com.globalids.util;

import java.util.SplittableRandom;

/**
 * Created by debasish paul on 05-11-2018.
 */
public class Test {
    public static void main(String[] args) {
        SplittableRandom random = new SplittableRandom();
        System.out.println(random.nextLong(9000000000l,9999999999l));
    }

}
