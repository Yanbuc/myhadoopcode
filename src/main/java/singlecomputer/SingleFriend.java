package singlecomputer;


import org.apache.commons.lang.StringUtils;

import java.io.*;

import java.lang.reflect.Array;
import java.util.*;

public class SingleFriend {
    public static void main(String[] args) throws IOException {
        File f=new File("D:/tt/friendsdata.txt");
        BufferedReader bf=new BufferedReader(new InputStreamReader(new FileInputStream(f)));
        String line="";
        String[] tmp;
        Map<String,String[]> a =new HashMap<String, String[]>();
        while ((line=bf.readLine())!=null){
              tmp=line.split(":");
              a.put(tmp[0],tmp[1].trim().split(","));
        }
        Map<String,String> share=new HashMap<String, String>();
        for(Map.Entry<String,String[]> entry:a.entrySet()){
            for(Map.Entry<String,String[]> en:a.entrySet()){
                if(en.getKey().equals(entry.getKey())){
                    continue;
                }
               tmp=calcaulateCommon(entry.getValue(),en.getValue());
                if(tmp.length==0){
                    continue;
                }
               share.put(entry.getKey()+"_"+en.getKey(), StringUtils.join(tmp,","));
            }
        }
        for(Map.Entry<String,String> b:share.entrySet()){
            System.out.println(b.getKey()+" : "+b.getValue());
        }

    }

    public static  String[] calcaulateCommon(String[] src,String[] dest){

       ArrayList<String> arr=new ArrayList<String>();
        for(int i=0;i<src.length;i++){
            for(int j=0;j<dest.length;j++){
                if(dest[j].equals(src[i])){
                    arr.add(dest[j]);
                }
            }
        }
        //System.out.println(Arrays.toString(src)+  "   "+Arrays.toString(dest)+"  "+Arrays.toString(arr.toArray(new String[arr.size()])));
        return arr.toArray(new String[arr.size()]);
    }

}
