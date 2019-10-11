package hadoopetl;
// PkGUU_ggO3k	theresident	704	Entertainment	262	11235	3.85	247	280	PkGUU_ggO3k	EYC5bWF0ss8	EUPHdnE83GY	JO1LTIFOkTw	gVSzbvFnVRY	l9NJ04JiZj4	ay3gcr84YeQ	AfBxANiGnnU	RyWz8hwGbY4	BeJ7tGRgiW4	fbq2-jd5Dto	j8fTx5E5rik	qGkCtXLN1W0	mh_MGyx9tgc	bgn6RYut2lE	HS6Nqxh4uf4	m9Gq44o5pcA	K7unV366Qr4	shU2hfHKmU0	p0lq5-8IDqY
public class EtlUtils {
    public static String getETLString(String str){
         StringBuilder sb=new StringBuilder();
         String[] fields=str.split("\t");
         if(fields.length<9){
             return null;
         }
         fields[3]=fields[3].replaceAll(" ","");
         for(int i=0;i<fields.length;i++){
             sb.append(fields[i]);
             if(i<9){
                 if(i<fields.length-1){
                     sb.append("\t");
                 }
             }else{
                 if(i<fields.length-1){
                     sb.append("&");
                 }
             }
         }
        return sb.toString();
    }

}
