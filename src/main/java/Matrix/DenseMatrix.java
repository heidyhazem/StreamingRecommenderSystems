package Matrix;

import java.util.*;

public class DenseMatrix {

    //public float[][] content;

    public List<List<Float>> contentL = new ArrayList<List<Float>>();


    //private ArrayList<String> firstDArray;
    //private ArrayList<String> secondDArray;

    private ArrayList<String> rows;
    private ArrayList<String> columns;

    //private Map<String,Integer> columnIndex = new HashMap<>();
    //private Map<String,Integer> rowIndex = new HashMap<>();



    /*//initialize size
    public DenseMatrix(Map<String,Long> rowIndex, Map<String,Long> columnIndex){
        //size is rows X columns
        content = new float[rowIndex.size()][columnIndex.size()];
        Set<String> rowKeySet = rowIndex.keySet();
        Set<String> colKeySet = rowIndex.keySet();
        firstDArray = new ArrayList<>(rowKeySet);
        secondDArray = new ArrayList<>(colKeySet);
    }

    //initialize size
    public DenseMatrix(ArrayList<String> rowIndex, ArrayList<String> columnIndex){
        //size is rows X columns
        content = new float[rowIndex.size()][columnIndex.size()];
        firstDArray = rowIndex;
        secondDArray = columnIndex;
    }

    //initialize size
    public DenseMatrix(int rowSize, int columnSize){
        //size is rows X columns
        content = new float[columnSize][rowSize];
    }

    //getColumnSize
    public int getColumnLength(){
        return content[0].length;
    }
    //getrowSize
    public int getRowLength(){
        return content.length;
    }

    //Resize one column and one row as we have received new user
    public DenseMatrix updateWithNewComer(String newComer){
        firstDArray.add(newComer);
        secondDArray.add(newComer);
        DenseMatrix largerOne = new DenseMatrix(firstDArray.size(),secondDArray.size());
        System.arraycopy(content, 0, largerOne.content, 0, content.length);

        return largerOne;
    }

    //Resize and remove comer
    public void removeComer(String comer){
        int rowIndex = firstDArray.indexOf(comer);
        int colIndex = secondDArray.indexOf(comer);


        float[][] newContent = new float[firstDArray.size()-1][secondDArray.size()-1];
        int ir;
        int jc ;
        Boolean rowFlag = false;
        Boolean columnFlag = false;

        for(int i=0 ; i<getRowLength(); i++){
            if(i == rowIndex){
                rowFlag = true;
                continue;

            }
            for(int j=0 ; j<getColumnLength(); j++){
                if (j == colIndex){
                    columnFlag = true;
                    continue;
                }

                if(!rowFlag && !columnFlag){
                    newContent[i][j] = content[i][j];
                }

            }
        }

        firstDArray.remove(comer);
        secondDArray.remove(comer);




    }

    //Set
     public void setValue (String row, String column, float value) throws Exception{
        try {
            int rowIndex = firstDArray.indexOf(row);
            int colIndex = secondDArray.indexOf(column);

            content[rowIndex][colIndex] = value;
        }
        catch (Exception e){
            System.out.println("the user is not in the list");
        }

     }

     //Set
    public void setValue(int row, int column, float value){
        content[row][column] = value;
    }

     //get
     public float getValue (String row, String column, float value) throws Exception{

         float returnedValue ;

         int rowIndex = firstDArray.indexOf(row);
         int colIndex = secondDArray.indexOf(column);

         returnedValue = content[rowIndex][colIndex];
         return returnedValue;
     }

    //get
    public float getValue (int row, int column, float value) throws Exception{

        float returnedValue ;
        returnedValue = content[row][column];
        return returnedValue;
    }


*/
    public DenseMatrix(Map<String,Long> rowIndex, Map<String,Long> columnIndex){
        //size is rows X columns
        Set<String> rowKeySet = rowIndex.keySet();
        Set<String> colKeySet = columnIndex.keySet();


        rows = new ArrayList<>(rowKeySet);
        columns = new ArrayList<>(colKeySet);
    }

    public void setValue (String row, String column, float value, Boolean rowIsKnown, Boolean colIsKnown) throws Exception{
        try {

            //TODO: FIND WAY TO ENFORCE ORDER
            int rowIndex = rows.indexOf(row);
            int colIndex = columns.indexOf(column);

            //content[rowIndex][colIndex] = value;
            //lw how gdid ybaa el index bt3o fi a5r
            //lw howa adem ybaa el index bt3o fi el nos
            //ana 3iza first we second array ykono mirror ll indices fi el list
            if(rowIsKnown && colIsKnown){

                List<Float> colList = contentL.get(rowIndex);
                colList.add(colIndex,value);
                contentL.set(rowIndex,colList);
            }

            else if(!colIsKnown &&  rowIsKnown ){

                //ex : A,B
                List<Float> colList = new ArrayList<>();
                colList.add(0,value);
                contentL.add(colList);
                //B,A
                List<Float> colList2 = contentL.get(rowIndex);
                colList2.add(colIndex,value);
                contentL.set(rowIndex,colList2);
            }

            else if(!rowIsKnown && colIsKnown){

                System.out.println("THE COLLLL INDEXXXX " +colIndex);
                System.out.println("THE row INDEXXXX "+ rowIndex);

                List<Float> colList = new ArrayList<>();
                colList.add(0,value);
                contentL.add(1,colList);
                //List<Float> colList2 = contentL.get(colIndex);
                //colList2.add(1,value);
                //contentL.set(colIndex,colList2);
            }
            else if(!rowIsKnown && !colIsKnown){
                List<Float> colList = new ArrayList<>();
                colList.add(colIndex,value);
                contentL.add(rowIndex,colList);
            }

            /*if(isKnown){
                List<Float> colList = contentL.get(rowIndex);
                colList.set(colIndex,value);
                contentL.set(rowIndex,colList);
            }

            if(!isKnown){
                List<Float> colList = new ArrayList<>();
                colList.add(colIndex,value);

                contentL.add(rowIndex,colList);

            }*/
        }
        catch (Exception e){
            System.out.println("the user is not in the list");
        }

    }

    public void addRow(List<Float> theRow, String element){
        int index = rows.indexOf(element);
        //row gahz
        contentL.set(index, theRow);
    }

    public void removeElement (String element){
        int index = rows.indexOf(element);
        contentL.remove(index);
        for (List<Float> col : contentL){
            col.remove(index);
        }

        rows.remove(index);
        columns.remove(index);
    }






}