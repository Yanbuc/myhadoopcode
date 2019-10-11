package kafkaproject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Tt {
    public static void main(String[] args) throws ParseException {
        String log="master|||192.168.249.19|||2019_04_09 19:13:12|||GET|||200|||http://192.168.249.19:8080/static/image/8_XImal3h5I/hqdefault.jpg|||Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36";
        String[] arr=StringUtils.getFields(log);
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy_MM_dd HH:mm:ss");
        Date parse = sdf.parse(arr[2]);
        Calendar c=Calendar.getInstance();
        c.setTime(parse);
        System.out.println(c.get(Calendar.DAY_OF_MONTH));


    }
}
