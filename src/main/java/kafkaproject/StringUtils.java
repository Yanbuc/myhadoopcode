package kafkaproject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

// 字符串分割
public class StringUtils {
     private static String token="\\|\\|\\|";

      public static  String filterLog(String log,String token,String spfg){
          String[] fields = log.split(token);
          if(fields.length <7 || fields ==null ){
              return null;
          }
          StringBuffer sb=new StringBuffer();
          for(int i=0;i<fields.length-1;i++){
              sb.append(fields[i]);
              sb.append(spfg);
          }
          sb.append(fields[fields.length-1]);
          return sb.toString();
      }
      public static String[] getFields(String log){
          return log.split(token);
      }
      public static Date str2Date(String str){
          try {
              SimpleDateFormat sdf=new SimpleDateFormat("yyyy_MM_dd HH:mm:ss");
              return sdf.parse(str);
          } catch (ParseException e) {
              e.printStackTrace();
          }
          return null;
      }
      public static String getVisitPage(String page){
          String[] ps=page.split("\\?");
          return ps[0];
      }

}
