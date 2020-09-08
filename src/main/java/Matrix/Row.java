package Matrix;

import java.io.Serializable;
import java.util.ArrayList;

public class Row  implements Serializable {
    //This is an alternating sorted array
    private ArrayList<Integer> content;
    private int length=0;

    public Row (int numbColumns)
    {
        length = numbColumns;
        for (int i = 0; i < numbColumns;i++)
            setColumnToZero(i);
    }

    public Row (int[] initialValues )
    {
        length = initialValues.length;
        content = new ArrayList<>(length);
        for (int i = 0; i < length;i++)
            setColumn(i, initialValues[i]);
    }

    public void setColumn(int columnNumber, int value)
    {
        //checkAndAdjustRowSize(columnNumber);
        if (value != 0 && value != 1)
            throw new IllegalArgumentException("values for columns are binary either zero or one");
        else if (value ==0)
            setColumnToZero(columnNumber);
        else
            setColumnToOne(columnNumber);
    }


    //TODO: whatever index it accepts
    //Solved
    public void setColumnToOne(int columnNumber)
    {
        checkAndAdjustRowSize(columnNumber);

        if (!content.contains(columnNumber))
            content.add(columnNumber);
        }

    private void checkAndAdjustRowSize(int columnNumber) {
        if (columnNumber+1 > length)
        {
            length=columnNumber+1;
        }
    }

    public void setColumnToZero(int columnNumber )
    {
       checkAndAdjustRowSize(columnNumber);
    }

    public void truncateSize(int newSize)
    {
        if (newSize < length)
        {

            for (int i = newSize; i <length; i++)
                content.remove(Integer.valueOf(i));
            length = newSize;
        }
    }

    public int getColumnValue(int columnNumber)
    {
        if (columnNumber +1 > length)
            return -1;

        if (content.contains(Integer.valueOf(columnNumber)))
            return 1;

        return 0;
    }
    public int size()
    {
        return length;
    }
    public int[] values()
    {
        int[] result = new int[length];
        for (int i = 0; i< length; i++)
        {
            if (content.contains(Integer.valueOf(i)))
                result[i] = 1;
            else
                result[i]=0;

        }
        return  result;
    }
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder(length);
        sb.append("[");
        for (int i : values())
        {

            sb.append(i);
            sb.append(", ");
        }
        sb.append("]");
        int index = sb.lastIndexOf(", ");
        sb.replace(index,index+2,"" );
        return sb.toString();
    }


    public void reomveColumn (int colNumber){
        content.remove(colNumber);
    }
}
