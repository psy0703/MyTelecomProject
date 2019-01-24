package com.ng.telecom.dataproducer;

import java.util.Random;

public class TelecomUtil {

    private static Random random = new Random();

    /**
     * 返回一个随机的大于等于 start ，小于等于 end 的随机整数
     * @param start
     * @param end
     * @return
     */
    public static int randomInt(int start, int end){
        if (start > end){
            throw new IllegalArgumentException("参数异常：start 不能大于 end ！");
        }
        //[0 , end] ==> [start , end]
        return random.nextInt(end - start + 1) + start;
    }
    /**
     * 生成随机的long整数
     * @param from
     * @param to
     * @return
     */
    public static long randomLong(long from , long to){
        if(from > to) throw new IllegalArgumentException("参数异常: from 不能大于to !");
        //[0,to - from ] [ from , to ]
        return Math.abs(random.nextLong()) % (to - from + 1) + from;
    }
}
