package Matrix;





import com.sun.org.apache.xpath.internal.operations.Bool;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

public class SparseBinaryMatrix  implements Serializable {

    private ArrayList<Row> content;

    private int rowLength;

    private HashMap<String, Integer> columnLabels;
    private HashMap<Integer, String> inverseColumnLabels;

    private HashMap<String, Integer> rowLabels;
    private HashMap<Integer, String> inverseRowLabels;

    private enum LabelerType{Row, Column};

    public Integer colNumber;
    public  Integer rowNumber;


    // This constructor initializes the matrix with zeros
    public SparseBinaryMatrix(int rows, int columns)
    {
        content = new ArrayList<>(rows);
        rowLength = columns;
//        for (int i = 0; i < rows; i++)
//            content.add(new Row(columns));


    }

    private void assignObjectLabel(int index, String label, HashMap<String, Integer> forward, HashMap<Integer, String> inverse
    , LabelerType type)
    {
        // Check if the column has already another label
        if (inverse == null && type == LabelerType.Column)
        {
            inverse = inverseColumnLabels =  new HashMap<>();
            forward = columnLabels = new HashMap<>();
        }
        else if (inverse == null & type == LabelerType.Row)
        {
            inverse = inverseRowLabels = new HashMap<>();
            forward = rowLabels = new HashMap<>();
        }
        Integer k = Integer.valueOf(index);
        String v =inverse.get(k);
        if (  v != null)
        {
            inverse.remove(k);
        }
        forward.remove(v);

        forward.put(label, k);
        inverse.put(k,label);
    }
    public void assignColumnLabel(int columnNumber, String label)
    {
        assignObjectLabel(columnNumber,label,columnLabels,inverseColumnLabels, LabelerType.Column);
    }

    public Set<String> getColumnLabels (){
        return columnLabels.keySet();
    }

    public void assignRowLabel(int rowNumber, String label)
    {
        assignObjectLabel(rowNumber,label, rowLabels, inverseRowLabels, LabelerType.Row);
    }

    public Set<String> getRowLabels (){
        return rowLabels.keySet();
    }



    public void setRow(int index, Row row)
    {
       if (index < content.size())
            content.set(index, row);
       else
           content.add(index, row);
        checkAndAdjustRowLength(row);
    }

    public void setRow(String rowLabel, Row row){
        int index = resolveRowLabel(rowLabel);

        if (index < content.size())
            content.set(index, row);
        else
            content.add(index, row);
        checkAndAdjustRowLength(row);
    }

    private void checkAndAdjustRowLength(Row row) {
        if (row.size() > rowLength) {
            rowLength = row.size();
            adjustRowsLength();
        }
        else if (row.size() < rowLength)
            adjustRowsLength();
    }

    public void appendRow(Row row)
    {
        content.add(row);
        checkAndAdjustRowLength(row);

    }

    private void adjustRowsLength()
    {
        Row r;
         for (int i = 0; i < content.size();i++)
         {
             r = content.get(i);
             int rowSize = r.size();
             if (rowSize< rowLength)
             {
                 for (int j = rowSize; j < rowLength;j++)
                 {
                     r.setColumnToZero(j);
                     //System.out.println(r.toString());

                 }
                 content.set(i,r);
             }
         }
    }

    private int resolveObjectLabel(String label, HashMap<String, Integer> forwards)
    {
        if (forwards == null)
            return -1;
        Integer objectNumber = forwards.get(label);
        if (objectNumber == null)
            return -1;
        return objectNumber.intValue();
    }
    private int resolveRowLabel(String rowLabel)
    {
        int result = resolveObjectLabel(rowLabel,rowLabels);
        if (result ==-1)
            throw new IllegalArgumentException("No row has the label "+ rowLabel);
        return result;
    }

    private int resolveColumnLabel(String columnLabel)
    {
        int result = resolveObjectLabel(columnLabel,columnLabels);
        if (result ==-1)
            throw new IllegalArgumentException("No column has the label "+ columnLabel);
        return result;
    }
    public MatrixSize size()
    {
        return  new MatrixSize(content.size(), rowLength);
    }
    private String getHeader()
    {

        StringBuilder sb = new StringBuilder();
        sb.append("Row [");
        boolean isInverseColumnLabelsNull = inverseColumnLabels == null;
        for (int i = 0; i < rowLength;i++)
        {
            String l = isInverseColumnLabelsNull ? null :inverseColumnLabels.get(Integer.valueOf(i));
            sb.append(l == null ?i:l);
            sb.append(", ");

        }
        int index = sb.lastIndexOf(", ");
        sb.replace(index,index+2,"" );
        sb.append("]");
        return sb.toString();
    }

    private String getRowLabel(int i)
    {
        boolean isInverseRowLabelNull = inverseRowLabels == null;
        return isInverseRowLabelNull? String.valueOf(i): inverseRowLabels.get(Integer.valueOf(i)) == null? String.valueOf(i): inverseRowLabels.get(Integer.valueOf(i));
    }
    public Row getRow(String rowLabel){
        int index = resolveRowLabel(rowLabel);
        return content.get(index);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(getHeader()).append(System.getProperty("line.separator"));
        int i = 0;
        for (Row r: content)
        {
            sb.append(getRowLabel(i)).append(" ").append(r.toString()).append(System.getProperty("line.separator"));
            i++;
        }
        return sb.toString();
    }

    public int getValue(String row, String column)
    {
        int colNumber = resolveColumnLabel(column);
        int rowNumber = resolveRowLabel(row);

        return content.get(rowNumber).getColumnValue(colNumber);
    }

    //returns items users rated positively
    public ArrayList<String> getPositiveColumns(String row){
        ArrayList<String> positiveColumns = new ArrayList<>();
        int rowNumber = resolveRowLabel(row);
        Row columnsValues = content.get(rowNumber);

        for(String colLabel : columnLabels.keySet()){
            if(columnsValues.getColumnValue(resolveColumnLabel(colLabel)) == 1){
                positiveColumns.add(colLabel);
            }
        }
        return positiveColumns;
    }

    public Boolean ratedPositively(String row, String col){
        return getPositiveColumns(row).contains(col);
    }

    public int getValue(int row, int column)
    {
        return content.get(row).getColumnValue(column);
    }
    public int getValue(String row, int column)
    {
        int rowNumber = resolveRowLabel(row);
        return content.get(rowNumber).getColumnValue(column);
    }
    public int getValue(int row, String column)
    {
        int colNumber = resolveColumnLabel(column);
        return content.get(row).getColumnValue(colNumber);
    }


    public void setValue (String row, String column)
    {
        int colNumber = resolveColumnLabel(column);
        int rowNumber = resolveRowLabel(row);

        //System.out.println(rowNumber);
        content.get(rowNumber).setColumnToOne(colNumber);

        }

     public void setValueWithCheck(String row, String column){//,SparseBinaryMatrix theMatrix){
        // row is the user and column is the item
        //user gdid
        if(!getRowLabels().contains(row)){
            //user gdid, item gdid
            if(!getColumnLabels().contains(column)){
                int [] myarray = new int[colNumber+1];
                Arrays.fill(myarray,0);
                this.appendRow(new Row(myarray));
                this.assignRowLabel(rowNumber,row);
                this.assignColumnLabel(colNumber,column);
                this.setValue(row,column);
                colNumber+=1;
                rowNumber+=1;
            }
            //user gdid , itmem adem
            else{

                this.appendRow(new Row(new int []{0}));
                this.assignRowLabel(rowNumber,row);
                this.setValue(row,column);
                rowNumber+=1;
            }

        } else {
            //user adem, item gdid
            if(!getColumnLabels().contains(column)){
                Row withputAddedColumn = this.getRow(row);
                withputAddedColumn.setColumn(colNumber,1);
                this.assignColumnLabel(colNumber,column);
                this.setRow(row,withputAddedColumn);
                colNumber+=1;

                //Receive User2,iTEM1
                this.setValue(row,column);

            }
            //user adem, item adem
            else{

                this.setValue(row,column);
            }
        }
     }

     public void removeRow (String row){
         int rowNumber = resolveRowLabel(row);
         content.remove(rowNumber);
         //rowLabels.remove(row);
     }

     public Boolean containsUser(String user){
        return rowLabels.containsKey(user);
     }





     /*public void removeColumn(String column){
         int colNumber = resolveColumnLabel(column);


         for(Row row : content){
             System.out.println("pss"+row.getColumnValue(colNumber));
         }
         //columnLabels.remove(column);


     }
*/

}
