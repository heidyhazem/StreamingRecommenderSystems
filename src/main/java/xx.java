
import Matrix.DenseMatrix;
import Matrixo.*;
import org.apache.calcite.sql.dialect.SybaseSqlDialect;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.expressions.In;
import org.checkerframework.checker.units.qual.A;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

import java.util.*;

// Java code to show implementation of
// Guava's Table interface
import java.util.Map;

import static com.google.common.base.Predicates.or;


public class xx {


    /*public static class Row
    {
        public Integer sij;
        public Integer i;
        public Integer j;
        public Row(Integer sij, Integer i, Integer j)
        {
            this.sij = sij;
            this.i = i;
            this.j = j;
        }
    }*/
    public static void main (String[] args) throws Exception {


        ArrayList<String> zz = new ArrayList<>();

        zz.add(0,"heidy");
        zz.add(1,"5afaga");
        zz.remove("heidy");
        zz.add("heidy");


       //System.out.println(zz.indexOf("heidy"));



        for (String m : zz){
            //System.out.println(m);
        }


        int [][] x = new int[2][2] ;
        x[0][0] = 5;
        x[0][1] = 3;

        /*for (int[] i : x){
            for (int j : i){
                System.out.println(j);
            }
        }*/

        /*int[][] originalArray = new int[2][2];
        int[][] largerArray = new int[3][2];

        originalArray[0][0] = 1;
        originalArray[0][1] = 2;
        originalArray[1][0] = 3;
        originalArray[1][1] = 5;

        System.arraycopy(originalArray, 0, largerArray, 0, originalArray.length);

        for (int[] i : originalArray){
            for (int j : i){
                System.out.println(j);
            }
        }


        for (int[] i : largerArray){
            for (int j : i){
                System.out.println(j);
            }
        }
*/

/*
        SparseMatrix a = new SparseMatrix(5);

        SparseVector x = new SparseVector(5);
        a.put(0, 1, 1.0);
        a.put(1, 1, 1.0);
        a.put(1, 2, 1.0);




        //Testing tableSaw
        double[] numbers = {1, 2, 3, 4};
        DoubleColumn nc = DoubleColumn.create("nc", numbers);
        System.out.println(nc.print());

        double three = nc.get(2);
        System.out.println(three);


        String[] animals = {"bear", "cat", "giraffe"};
        String[] users = {"bear", "cat", "giraffe"};
        double[] cuteness = {90.1, 84.3, 99.7};

        Table table =
                Table.create("Cute Animals")
                        .addColumns(
                                StringColumn.create("items", animals),
                                DoubleColumn.create("rating", cuteness),
                                StringColumn.create("users",users));

        StringColumn i = table.stringColumn("items");
        StringColumn u = table.stringColumn("users");
        Table test = table.where(i.isEqualTo("bear").and(u.isEqualTo("bear")));

        for (String b : table.stringColumn("items").asList()){
            //System.out.println(b);
        }

        i = i.set(0,"heidy");


        //System.out.println(i);


        Map<String,Map<String,Float>> tex = new HashMap<>();

        //stucture2
        Map<String,ArrayList<String>> tex2 = new HashMap<>();



        Map<String,Float> one = new HashMap<>();
        one.put("Heidy",5f);


        // For structure 2
        ArrayList<String> onelist = new ArrayList<>();
        onelist.add("Heidy");

        Map<String,Float> two = new HashMap<>();
        one.put("Heidy",50f);

        ArrayList<String> twolist = new ArrayList<>();
        twolist.add("Heidy");



        Map<String,Float> three3 = new HashMap<>();
        one.put("hhh",30f);

        ArrayList<String> threelist = new ArrayList<>();
        threelist.add("hhh");



        tex.put("user1",one);
        tex.put("user2",two);
        tex.put("user3",three3);


        tex2.put("user1",onelist);
        tex2.put("user2",twolist);
        tex2.put("user3",threelist);

        Map<String,Float> backone =  tex.get("user1");
        backone.put("Hamada",3f);
        tex.put("user1",backone);


        //System.out.println(xxxx);

        Object[] z = tex.values().toArray();



        //System.out.println(tex.values().toArray()[0]);





        System.out.println(tex2.values().toArray()[2]);

        //(user x , item i)*/

        /*int b [][] = new int[2][2];



        DenseMatrix m = new DenseMatrix(2,2);
        m.setValue(0,0,4);*/



        /*float[][] arr = new float[2][2];
        //arr[1][0] = 9F;

        for (float[] i : arr){
            for (float j : i){
                System.out.println(j);

            }
        }*/

       /* DenseMatrix m = new DenseMatrix(2,2);
        m.setValue(1,0,9F);
        *//*for (float[] i : m.content){
            for (float j : i){
                System.out.println(j);

            }
        }*//*

        Map<String,Long> users = new HashMap<>();
        users.put("Heidy",5L);
        users.put("ramy",8L);

        DenseMatrix mm = new DenseMatrix(users,users);
        mm.setValue("Heidy","Heidy",5F);
        mm.setValue("ramy","Heidy",9F);
        mm.setValue("ramy","ramy",10F);

        *//*for (float[] i : mm.content){
            for (float j : i){
                System.out.println(j);

            }
        }*//*
*/
       /* DenseMatrix nm = mm.updateWithNewComer("sasa");
        for (float[] i : nm.content){
            for (float j : i){
                System.out.println(j);

            }
        }

*/


        /*List<List<Float>> listOfLists = new ArrayList<List<Float>>();

        List<Float> c = new ArrayList<>();
        c.add(5F);
        c.add(6F);
        c.add(8F);

        List<Float> b = new ArrayList<>();
        b.add(3F);
        b.add(7F);
        b.add(9F);

        List<Float> a = new ArrayList<>();
        a.add(3F);
        a.add(7F);
        a.add(9F);



        listOfLists.add(c);
        listOfLists.add(1,b);
        listOfLists.add(a);

        float p = listOfLists.get(0).get(1);


        int index = 2;

        //listOfLists.remove(2);
        //List<List<Float>>  lisc = (List<List<Float>>) ((ArrayList<List<Float>>) listOfLists).clone();
        for (List<Float> i : listOfLists){
            i.remove(2);
        }

        for (List<Float> i : listOfLists){
            System.out.println(i.size());
        }*/

        Map<String,Long> one = new HashMap<>();
        //galy user heidy
        one.put("Heidy",5L);

        DenseMatrix mm = new DenseMatrix(one,one);
        mm.setValue("Heidy","Heidy",3F,false,false);

        /*List<Float> xx = new ArrayList<>();
        xx.add(7F);*/

        System.out.println(mm.contentL.size());



        /*for (List<Float> i : mm.contentL){
            System.out.println("Row index is : "+mm.contentL.indexOf(i));
            for(float j : i){
                System.out.println("is is"+j);
                System.out.println("columns index is : "+i.indexOf(j));
            }
        }*/

        one.put("Ahmed",5L);
        mm.setValue("Ahmed","Heidy",9F,false,true);

       for (List<Float> i : mm.contentL){
           System.out.println("Row index is : "+mm.contentL.indexOf(i));
           for(float j : i){
               System.out.println("is is"+j);
               System.out.println("columns index is : "+i.indexOf(j));
           }




        }



         /*mm.setValue("Ahmed","Ahmed",10F,true,true);
        for (List<Float> i : mm.contentL){
            //System.out.println("size of list is "+i.size());
            for(float j : i){
                System.out.println("is is"+j);
            }
        }

        System.out.println(mm.contentL.get(1).size());


*/

















        // System.out.println("my p is "+p);


        //I have received new user sasa










        /*int[] arr = new int[5];
        Arrays.fill(arr, -1);*/
        //System.out.println(Arrays.toString(arr));  //[-1, -1, -1, -1, -1 ]

        /*for (int[] i : b){
            for (int j : i){
                System.out.println(j);
            }
        }

*/

        System.out.println("NEW NEW NEW NEW NEW NEW NEW NEW NEW NEW NEW NEW NEW NEW New");


        Map<String,Tuple2<Integer,ArrayList<Float>>> usersTest = new HashMap<>();
        int counter = 0;

        //New Users "heidy"


        if(! usersTest.containsKey("heidy") && counter==0){
            ArrayList<Float> newArr = new ArrayList<Float>();
            newArr.add(0,1F);
            usersTest.put("Heidy",Tuple2.of(counter,newArr));
            counter = counter+1;
        }


        if(counter > 0 && !usersTest.containsKey("Ahmed")){

            for(Map.Entry<String,Tuple2<Integer,ArrayList<Float>>> xmap : usersTest.entrySet() ){
                if(!xmap.getKey().equals("Ahmed")){
                    //sim
                    ArrayList<Float> newArr = new ArrayList<Float>();
                    newArr.add(xmap.getValue().f0,2F);
                    newArr.add(counter,1F);
                    usersTest.put("Ahmed",Tuple2.of(counter,newArr));

                    ArrayList<Float> newArr2 = xmap.getValue().f1;
                    newArr2.add(2F);
                    usersTest.put("Heidy",Tuple2.of(xmap.getValue().f0,newArr2));
                    counter = counter+1;


                }

                if(!xmap.getKey().equals("noha")){
                    //sim
                    ArrayList<Float> newArr = new ArrayList<Float>();
                    newArr.add(xmap.getValue().f0,2F);
                    newArr.add(counter,1F);
                    usersTest.put("Ahmed",Tuple2.of(counter,newArr));

                    ArrayList<Float> newArr2 = xmap.getValue().f1;
                    newArr2.add(2F);
                    usersTest.put("Heidy",Tuple2.of(xmap.getValue().f0,newArr2));


                }

                else {
                    System.out.println("hiiiiii");
                    ArrayList<Float> newArr = xmap.getValue().f1;
                    newArr.add(counter,1F);
                    usersTest.put(xmap.getKey(),Tuple2.of(xmap.getValue().f0,newArr));

                }
            }

        }

        System.out.println(counter);

        for(Map.Entry<String,Tuple2<Integer,ArrayList<Float>>> xmap : usersTest.entrySet() ){
            System.out.println(xmap.getKey() +" and the size is  "+ xmap.getValue().f1.size() );
            for(Float f : xmap.getValue().f1){
                System.out.println("Thr  value is "+f);

            }
        }







    }







}
