package com.ng.telecom.dataproducer;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class CallRecord {

    private String call1;//电话号码1
    private String call2;//电话号码2
    private long startTime; //通话开始时间
    private int duration; //通话时长（秒）
    /**
     * 用来表示call1是主叫还是被叫： 0代表call1 是主叫，1 代表call 1 是被叫
     */
    private int flag = 0;

    public CallRecord(){
        //生成需要的数据
        //1.生成call1 和 call2
        String[] call1AndCall2 = produceCall1AndCall2();
        this.setCall1(call1AndCall2[0]);
        this.setCall2(call1AndCall2[1]);
        //2.生成通话开始时间
        this.setStartTime(produceTime());
        //3.生成通话时长（最少5秒最多9999）
        this.setDuration(TelecomUtil.randomInt(5, 9999));
    }

    public String getCall1() {
        return call1;
    }

    public void setCall1(String call1) {
        this.call1 = call1;
    }

    public String getCall2() {
        return call2;
    }

    public void setCall2(String call2) {
        this.call2 = call2;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }
    // call1,call2,starttime,duration,flag
    // 110,112,111999,0020,1
    @Override
    public String toString() {
        String str = call1 + "," + call2 + "," + startTime + "," + duration + "," + flag;
        return str;
    }

    /**
     * 生成两个不重复的电话号码
     */
    private String[] produceCall1AndCall2(){
        List<String> phones = Data.phones;
        int index1 = TelecomUtil.randomInt(0, phones.size() - 1);
        int index2 = 0;
        while (true){
            index2 = TelecomUtil.randomInt(0, phones.size() - 1);
            if (index1 != index2) break;
        }
        return new String[]{phones.get(index1) , phones.get(index2)};
    }

    /**
     * 生成一个随机的时间戳（2018-2019）
     * @return
     */
    private long produceTime(){
        //2018 - 1 - 1
        Calendar c1 = Calendar.getInstance();
        c1.set(2018, 0, 1, 0, 0, 0);

        //2019 - 1 - 1
        Calendar c2 = Calendar.getInstance();
        c2.set(2019, 0, 1, 0, 0, 0);
        return TelecomUtil.randomLong(c1.getTimeInMillis(),
                c2.getTimeInMillis());
    }

    public static void main(String[] args) {
        CallRecord callRecord = new CallRecord();
        System.out.println(callRecord.getStartTime());
        SimpleDateFormat format = new SimpleDateFormat("yyyy年MM月dd日");
        System.out.println(format.format(new Date(callRecord.getStartTime())));
    }
}
