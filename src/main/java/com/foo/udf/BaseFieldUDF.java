package com.foo.udf;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

/*
 * Created by liucahnghao on 2020/1/8
 */
public class BaseFieldUDF extends UDF {
    public String evaluate(String line,String jsonkeyString){

        // 0 准备一个 sb
        StringBuilder sb = new StringBuilder();

        // 获取jsonKeys
        String[] jsonkeys = jsonkeyString.split(",");

        // 处理line
        String[] logContents = line.split("\\|");

        // 合法性的校验
        if (logContents.length != 2 || StringUtils.isBlank(logContents[1])) {
            return "";
        }

        try{
            //开始处理json
            JSONObject jsonObject = new JSONObject(logContents[1]);

            // 获取 cm 里面的对象
            JSONObject base = jsonObject.getJSONObject("cm");

            // 循环遍历取值
            for (int i = 0; i < jsonkeys.length; i++) {
                String filedName = jsonkeys[i].trim();
                if (base.has(filedName)) {
                    sb.append(base.getString(filedName)).append("\t");
                } else {
                    sb.append("").append("\t");
                }
            }
            sb.append(jsonObject.getString("et")).append("\t");
            sb.append(logContents[0]).append("\t");
        }catch (JSONException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        String line = "1577435693383|{\"cm\":{\"ln\":\"-49.0\",\"sv\":\"V2.8.1\",\"os\":\"8.0.3\",\"g\":\"KQ66GMA3@gmail.com\",\"mid\":\"m483\",\"nw\":\"WIFI\",\"l\":\"en\",\"vc\":\"7\",\"hw\":\"640*960\",\"ar\":\"MX\",\"uid\":\"u287\",\"t\":\"1577418216848\",\"la\":\"-23.0\",\"md\":\"HTC-5\",\"vn\":\"1.2.2\",\"ba\":\"HTC\",\"sr\":\"O\"},\"ap\":\"gmall\",\"et\":[{\"ett\":\"1577381033703\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"24\",\"action\":\"2\",\"extend1\":\"\",\"type\":\"2\",\"type1\":\"\",\"loading_way\":\"1\"}},{\"ett\":\"1577363288551\",\"en\":\"ad\",\"kv\":{\"entry\":\"1\",\"show_style\":\"4\",\"action\":\"5\",\"detail\":\"\",\"source\":\"3\",\"behavior\":\"2\",\"content\":\"1\",\"newstype\":\"9\"}},{\"ett\":\"1577391530805\",\"en\":\"active_foreground\",\"kv\":{\"access\":\"\",\"push_id\":\"2\"}},{\"ett\":\"1577430656140\",\"en\":\"favorites\",\"kv\":{\"course_id\":0,\"id\":0,\"add_time\":\"1577388657671\",\"userid\":8}}]}";
        String test = new BaseFieldUDF().evaluate(line, "mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t");
        System.out.println(test);

    }

}
