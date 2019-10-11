package sg;

public class TestArr {
    public static void main(String[] args) {
        int[] one=new int[10];
        for(int i=0;i<10;i++){
            one[i]=i;
        }
        int[] b=new int[one.length];
        System.arraycopy(one,0,b,0,10);
        for(int j:b){
            System.out.println(j);
        }
        b[1]=3;
        System.out.println(one[1]);

    }
}
