package com.szubd.rsp.tools;

import com.alibaba.fastjson.JSONObject;
import com.szubd.rsp.sparkInfo.appInfo.ExecutorInfo;

import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.TimeZone;

/**
 * @projectName: octopus-web
 * @package: com.szubd.rsp.tools
 * @className: MakeUpUtils
 * @author: Chandler
 * @description: 处理sparkInfo的工具类
 * @date: 2023/8/23 下午 5:18
 * @version: 1.0
 */
public class MakeUpUtils {
    // 自定义的比较器
    public static Comparator<ExecutorInfo.ExecutorSummary> executorComparator = (o1, o2) -> {
        // driver排在前面
        if ("driver".equals(o1.id)){
            return -Integer.MAX_VALUE;
        }else if("driver".equals(o2.id)){
            return Integer.MAX_VALUE;
        }
        try {
            return Integer.compare(Integer.parseInt(o1.id),Integer.parseInt(o2.id));
        } catch (NumberFormatException e){
            // 默认比较器：使用字符顺序比较
            return o1.id.compareTo(o2.id);
        }
    };
    // 日期格式
    public final static SimpleDateFormat input_sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'GMT'");
    public final static SimpleDateFormat output_sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public final static int GUARANTEE_TIME = 10*1000;
    static {
        output_sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));
        input_sdf.setTimeZone(TimeZone.getTimeZone("GMT+0"));
    }
    // 日期格式化操作
    public static String formatDateString(String input){
        try {
            return output_sdf.format(input_sdf.parse(input));
        } catch (Exception e) {
            return null;
        }
    }
    // 将子节文本人性化显示
    public static String makeUpString4Bit(JSONObject jsonObject, String key){
        long longValue = jsonObject.getLongValue(key);
        String res;
        if (longValue > 1024){
            if (longValue > 1024*1024){
                if (longValue > 1024*1024*1024){
                    res = (longValue/(1024*1024*1024)) + "GB";
                }else {
                    res = (longValue/(1024*1024)) + "MB";
                }
            }else {
                res = (longValue/1024) + "KB";
            }
        }else {
            res = longValue + "B";
        }
        return res;
    }
    // 将时长文本人性化显示
    public static String makeUpString4Time(JSONObject jsonObject, String key){
        long longValue = jsonObject.getLongValue(key);
        String res;
        if (longValue >= 1000){
            if (longValue >= 1000*60){
                if (longValue >= 1000*60*60){
                    res = (longValue/(1000*60*60)) + "h";
                }else {
                    res = (longValue/(1000*60)) + "min";
                }
            }else {
                res = (longValue/1000) + "s";
            }
        }else {
            res = longValue + "ms";
        }
        return res;
    }
    // 将带括号的文本人性化显示：去除括号
    public static String makeUpString4StringWithBracket(JSONObject jsonObject, String key){
        String string = jsonObject.getString(key);
        String res;
        if (string.length()<=2){
            res = null;
        }else {
            res = string.substring(1, string.length()-1);
        }
        return res;
    }
}
