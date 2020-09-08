import Matrix.Row;
import Matrix.SparseBinaryMatrix;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class TestSparseBinaryMatrix {

    public static void main(String[] args) {

        Boolean flag = true;
        SparseBinaryMatrix initialMatrix = new SparseBinaryMatrix(1,1);

        try
        {
            File file=new File("../../../ml100.txt");    //creates a new file instance
            FileReader fr=new FileReader(file);   //reads the file
            BufferedReader br=new BufferedReader(fr);  //creates a buffering character input stream
            //StringBuffer sb=new StringBuffer();    //constructs a string buffer with no characters
            String line;



            while((line=br.readLine())!=null)
            {
                //System.out.println(line);

                String user = line.trim().split(",")[0];
                String item = line.trim().split(",")[1];



                if(flag){
                    flag = false;
                    //initialMatrix = new SparseBinaryMatrix(1,1);

                    initialMatrix.appendRow(new Row(new int[]{0}));
                    initialMatrix.assignRowLabel(0,user);
                    initialMatrix.assignColumnLabel(0,item);
                    initialMatrix.setValue(user,item);


                    //update them
                    initialMatrix.colNumber = 1;
                    initialMatrix.rowNumber = 1;
                }

                else{
                    initialMatrix.setValueWithCheck(user,item);
                }

                System.out.println(initialMatrix.size());

                //sb.append(line);      //appends line to string buffer
                //sb.append("\n");     //line feed
            }
            fr.close();    //closes the stream and release the resources
            //System.out.println("Contents of File: ");
            //System.out.println(sb.toString());   //returns a string that textually represents the object
        }
        catch(IOException e)
        {
            e.printStackTrace();
        }
    }
}