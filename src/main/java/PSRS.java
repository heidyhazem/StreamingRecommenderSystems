import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import readingSources.SourceWithTimestamp;
import partitioning.*;

public class PSRS {

    public static void main (String[] args) throws Exception{

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);

        //set streaming execution environment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        /**
         Holds initialization of the inputStream
         */
        DataStream<Tuple3<String, String, Float>> inputStream = null;


        //***************************************Adding source(txt file (user,item,rate,timestamp))********************************
        if (params.has("input") && params.has("records")) {
            inputStream = env.addSource(new SourceWithTimestamp(params.get("input"), params.getInt("records")));
        } else if (params.has("input") && !params.has("records")) {
            inputStream = env.addSource(new SourceWithTimestamp(params.get("input")));
        } else {
            System.out.println("Use --input to specify file input  or use --records to specify records");
        }

        //***************************************Partitioning part******************************************************************

        /**
         Generate unified key for all records for batch partitioning
         */
        DataStream<Tuple4<Integer,String,String,Float>> withKeyedStream = new Batch(inputStream).generateOneKey(inputStream);


        //*************************************** Recommendation part******************************************************************



        //***************************************writing the output (sink)******************************************************************
        if(params.has("output")){
            withKeyedStream.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE);
        }
        else {
            System.out.println("Use --output to specify file input ");
        }



        env.execute("TenscentRec central");
    }
}