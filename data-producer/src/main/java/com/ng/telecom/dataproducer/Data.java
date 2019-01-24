package com.ng.telecom.dataproducer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 存储电话号码和联系人信息
 */
public class Data {

    //存储电话信息
    public static List<String > phones = new ArrayList<String>();
    //存储联系人信息
    public static Map<String ,String > contacts = new HashMap<String, String>();

    static {
        phones.add("15369468720");
        phones.add("19920860202");
        phones.add("18411925860");
        phones.add("14473548449");
        phones.add("18749966182");
        phones.add("19379884788");
        phones.add("19335715448");
        phones.add("18503558939");
        phones.add("13407209608");
        phones.add("15596505995");
        phones.add("17519874292");
        phones.add("15178485516");
        phones.add("19877232369");
        phones.add("18706287692");
        phones.add("18944239644");
        phones.add("17325302007");
        phones.add("18839074540");
        phones.add("19879419704");
        phones.add("16480981069");
        phones.add("18674257265");
        phones.add("18302820904");
        phones.add("15133295266");
        phones.add("17868457605");
        phones.add("15490732767");
        phones.add("15064972307");

        contacts.put("15369468720", "李雁");
        contacts.put("19920860202", "卫艺");
        contacts.put("18411925860", "仰莉");
        contacts.put("14473548449", "陶欣悦");
        contacts.put("18749966182", "施梅梅");
        contacts.put("19379884788", "金虹霖");
        contacts.put("19335715448", "魏明艳");
        contacts.put("18503558939", "华贞");
        contacts.put("13407209608", "华啟倩");
        contacts.put("15596505995", "仲采绿");
        contacts.put("17519874292", "卫丹");
        contacts.put("15178485516", "戚丽红");
        contacts.put("19877232369", "何翠柔");
        contacts.put("18706287692", "钱溶艳");
        contacts.put("18944239644", "钱琳");
        contacts.put("17325302007", "缪静欣");
        contacts.put("18839074540", "焦秋菊");
        contacts.put("19879419704", "吕访琴");
        contacts.put("16480981069", "沈丹");
        contacts.put("18674257265", "褚美丽");
        contacts.put("18302820904", "孙怡");
        contacts.put("15133295266", "许婵");
        contacts.put("17868457605", "曹红恋");
        contacts.put("15490732767", "吕柔");
        contacts.put("15064972307", "冯怜云");
    }
}
