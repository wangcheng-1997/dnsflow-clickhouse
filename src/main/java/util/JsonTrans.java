package util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.zjtuojing.dnsflow.BeanObj;

/**
 * ClassName IntelliJ IDEA
 * Date 2020/6/24 22:06
 */
public class JsonTrans {
    public static String EsDataBeanDd2Json(BeanObj.EsDataBeanDd esDataBeanDD) {
        String string = JSON.toJSONString(esDataBeanDD,new SerializeConfig(true));
        return string;
    }

    public static BeanObj.EsDataBeanDd Json2EsDataBeanDd(String jsonString) {
        return JSON.parseObject(jsonString, BeanObj.EsDataBeanDd.class);
    }

    public static String EsDnsUserInfoDd2Json(BeanObj.EsDnsUserInfoDd esDnsUserInfoDd) {
        String string = JSON.toJSONString(esDnsUserInfoDd,new SerializeConfig(true));
        return string;
    }

    public static BeanObj.EsDnsUserInfoDd Json2EsDnsUserInfoDd(String jsonString) {
        return JSON.parseObject(jsonString, BeanObj.EsDnsUserInfoDd.class);
    }
}
