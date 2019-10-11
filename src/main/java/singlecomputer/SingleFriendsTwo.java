package singlecomputer;

import java.io.*;
import java.util.*;

public class SingleFriendsTwo {
    public static void main(String[] args) throws IOException {
        File f=new File("D:/tt/friendsdata.txt");
        BufferedReader bf=new BufferedReader(new InputStreamReader(new FileInputStream(f)));
        String line="";
        String[] tmp;
        Map<String,String[]> a =new HashMap<String, String[]>();
        while ((line=bf.readLine())!=null){
            tmp=line.split(":");
            if(tmp.length<2){
                continue;
            }
            a.put(tmp[0],tmp[1].trim().split(","));
        }
        Map<String, HashSet<String>> fu=new HashMap<String, HashSet<String>>();
        for(Map.Entry<String,String[]> one:a.entrySet()){
            String user=one.getKey();
            for(String friend:one.getValue()){
                HashSet<String> users=fu.get(friend);
                if(users==null||users.size()==0){
                    users=new HashSet<String>();
                }
                users.add(user);
                fu.put(friend,users);
            }
        }
        Map<String,HashSet<String>> ends=new HashMap<String, HashSet<String>>();
        for(Map.Entry<String,HashSet<String>> enty:fu.entrySet()){
                 String[] tp=calculate(enty.getValue());
                 for(String s:tp){
                     HashSet<String> aa=ends.get(s);
                     if(aa==null||aa.size()==0){
                         aa=new HashSet<String>();
                     }
                     aa.add(enty.getKey());
                     ends.put(s,aa);
                 }
        }
        for(Map.Entry<String,HashSet<String>> hh:ends.entrySet()){
            System.out.println(hh.getKey()+"  "+ Arrays.toString(hh.getValue().toArray()));
        }

    }
    public static String[] calculate(HashSet<String> elenment){
        String[] arr=elenment.toArray(new String[elenment.size()]);
        List<String> retn=new ArrayList<String>();
        for(int i=0;i<arr.length-1;i++){
            for(int j=i+1;j<arr.length;j++){
                retn.add(arr[i]+"_"+arr[j]);
            }
        }
        return retn.toArray(new String[retn.size()]);
    }

}
