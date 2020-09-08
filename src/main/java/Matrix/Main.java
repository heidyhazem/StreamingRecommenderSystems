package Matrix;

import com.sun.org.apache.xml.internal.security.keys.keyresolver.implementations.DEREncodedKeyValueResolver;

import java.util.Arrays;

public class Main {

    public static void main(String[] args) {


//        testSingleRow();

        testMatrix();

    }

    private static void testMatrix() {

       /* Row row1 = new Row(new int[] {0,0,0,1,0,1});
        Row row2 = new Row(new int[] {1,1,1,1,1,1});
        Row row3 = new Row(new int[] {0,0,0,0,0,1});
        Row row4 = new Row(new int[] {1,1,1});

        SparseBinaryMatrix matrix = new SparseBinaryMatrix(2, row1.size());
        for (int i = 0; i < row1.size();i++)
            matrix.assignColumnLabel(i, "col"+i);

        matrix.assignRowLabel(0, "row1");
        matrix.assignRowLabel(1, "row2");
        System.out.println("Matrix size is "+matrix.size().toString());
        matrix.setRow(0, row1);
        matrix.setRow(1, row2);
        System.out.println("Matrix size is "+matrix.size().toString());

        System.out.println("Adding a new row at the end");
        matrix.appendRow(row3);
        System.out.println("Matrix size is "+matrix.size().toString());

        System.out.println("Adding a short row");
        matrix.appendRow(row4);
        System.out.println("Matrix size is "+matrix.size().toString());
        System.out.println(matrix.toString());

        System.out.println("Get the value of cell row1 x col2");
        matrix.setValue("row1","col2");
        System.out.println(matrix.getValue("row1","col2"));
        System.out.println(matrix.toString());

        if(matrix.getRowLabels().contains("row1")){
            System.out.println("yess");
        }

        for(String label : matrix.getColumnLabels()){
            System.out.println(label);
        }

        for(String label : matrix.getRowLabels()){
            System.out.println(label);
        }
        */

        System.out.println("test test test test test test test test test test test");

        /*//Assume I received first user item pair
        SparseBinaryMatrix matrixTest = new SparseBinaryMatrix(1, 1);
        matrixTest.assignRowLabel(0, "movie1");
        matrixTest.assignColumnLabel(0,"user1");

        System.out.println("Matrix size is "+matrixTest.size().toString());

        //I have to set index in valueState

        matrixTest.appendRow(new Row(new int[] {0}));

        matrixTest.assignRowLabel(1,"movie2");

        System.out.println("Matrix size is "+matrixTest.size().toString());

        matrixTest.assignColumnLabel(1,"user2");
        //matrixTest.setValue("movie2","user2");
        matrixTest.appendRow(new Row(new int[]{0,0}));
        matrixTest.appendRow(new Row(new int[]{0,1,0}));
        System.out.println("Matrix size is "+matrixTest.size().toString());
*/
        /*//List<Integer> arr = Arrays.asList(new Integer[2]);
        matrixTest.appendRow(new Row(new int[]{0,0}));

        System.out.println("Matrix size is "+matrixTest.size().toString());*/

        //Mimic a scenario (user1,item1),(user2,item2),(user3,item3)
        //for the first time
        Integer colNumber = 0;
        Integer rowNumber = 0;

        //Receive (user1,item1)
        SparseBinaryMatrix matrixTest3 = new SparseBinaryMatrix(1, 1);
        matrixTest3.appendRow(new Row(new int[]{0}));
        matrixTest3.assignRowLabel(rowNumber,"user1");
        matrixTest3.assignColumnLabel(colNumber,"item1");
        matrixTest3.setValue("user1","item1");

        //update them
        colNumber+=1;
        rowNumber+=1;

        //Receive (user2,item2)
        //eletnen godad

        if(! matrixTest3.getRowLabels().contains("user2")){
            //lw msh mwgod ybaa m7taga azwd row
            //bs hal m7taga azwd column wla laa
            if(! matrixTest3.getColumnLabels().contains("item2")){
                //add row and column in the array
                int [] myarray = new int[colNumber+1];
                Arrays.fill(myarray,0);
                matrixTest3.appendRow(new Row(myarray));
                matrixTest3.assignRowLabel(rowNumber,"user2");
                matrixTest3.assignColumnLabel(colNumber,"item2");
                matrixTest3.setValue("user2","item2");
                colNumber+=1;
                rowNumber+=1;
            }
        }

        //Receive(user3,item2)
        //user gdid(row) lkn el item adem (column)
        if(! matrixTest3.getRowLabels().contains("user3")){
            if(matrixTest3.getColumnLabels().contains("item2")){
                matrixTest3.appendRow(new Row(new int []{0}));
                matrixTest3.assignRowLabel(rowNumber,"user3");
                matrixTest3.setValue("user3","item2");
                rowNumber+=1;
            }
        }

        //Receive(user3,item3)
        //user adem el item gdid
        Row withputAddedColumn = matrixTest3.getRow("user3");
        withputAddedColumn.setColumn(colNumber,1);
        matrixTest3.assignColumnLabel(colNumber,"item3");
        matrixTest3.setRow("user3",withputAddedColumn);
        colNumber+=1;

        //Receive User2,iTEM1
        matrixTest3.setValue("user1","item2");

        //matrixTest3.setValue("user1","item4");

        if(! matrixTest3.getRowLabels().contains("user4")){
            //lw msh mwgod ybaa m7taga azwd row
            //bs hal m7taga azwd column wla laa
            if(! matrixTest3.getColumnLabels().contains("item4")){
                //add row and column in the array
                int [] myarray = new int[colNumber+1];
                Arrays.fill(myarray,0);
                matrixTest3.appendRow(new Row(myarray));
                matrixTest3.assignRowLabel(rowNumber,"user4");
                matrixTest3.assignColumnLabel(colNumber,"item4");
                matrixTest3.setValue("user4","item4");
                colNumber+=1;
                rowNumber+=1;
            }
        }


       //System.out.println(matrixTest3.toString());

        /*matrixTest3.removeRow("user2");

        System.out.println(matrixTest3.toString());

        */

        SparseBinaryMatrix newOne = new SparseBinaryMatrix(1,1);

        System.out.println(newOne.size());




        newOne.appendRow(new Row(new int[]{0}));
        newOne.assignRowLabel(0,"user1");
        newOne.assignColumnLabel(0,"item1");
        newOne.setValue("user1","item1");


        //update them
        newOne.colNumber = 1;
        newOne.rowNumber = 1;

        newOne.setValueWithCheck("user1","item2");
        newOne.setValueWithCheck("user2","item1");
        newOne.setValueWithCheck("user3","item3");
        newOne.setValueWithCheck("user3","item1");

        for(String i : newOne.getPositiveColumns("user3")){
            System.out.println(i);
        }





        System.out.println(newOne.toString());





        //test.setValue("user1","item1",3f);

        /*System.out.println("Matrix size is "+test.size().toString());
        System.out.println("Matrix is" + test.toString());
        System.out.println();

        //test.setRow(0,new DenseRow(new Float[]{7f}));


        for(Float i : test.getRow("user1").values()){
            System.out.println("The valuse is " + i);
        }

        System.out.println(test.getValue("user1","item1"));

        //System.out.println(test.getRow("user1").toString());
*/
        //System.out.println(test.getRow("user1").toString());









/*

        System.out.println("Matrix size is "+matrixTest3.size().toString());
        System.out.println("Matrix is" + matrixTest3.toString());
*/

























    }

    private static void testSingleRow() {
        Row row = new Row(new int[] {0,0,0,1,0,1});
        System.out.println("Row size is "+row.size());
        for (int i = 0; i < row.size();i++)
            System.out.println("Element at column "+ i + " is "+row.getColumnValue(i));
        System.out.println(row.toString());
        System.out.println("Changing the first column to 1");
        row.setColumnToOne(0);
        System.out.println("Get a column that does not exist");
        System.out.println(row.getColumnValue(999));
        System.out.println("Convert to the traditional vector");
        System.out.println(row.toString());
        System.out.println("Truncate to three columns only");
        row.truncateSize(3);
        System.out.println(row.toString());
    }
}
