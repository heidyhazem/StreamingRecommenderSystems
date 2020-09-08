package Matrix;

public class MatrixSize {

    private int rows;
    private int columns;

    public MatrixSize(int r, int c)
    {
        rows = r;
        columns = c;
    }
    public String toString()
    {
        return String.format("[%s x %s]", rows, columns);
    }
}
