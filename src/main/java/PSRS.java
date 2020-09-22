import Recommender.TenRec;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//local imports
import ReadingSources.SourceWithTimestamp;
import Partitioning.*;
//import recommender.*;
import Evaluation.*;
import Recommender.DISGD;

import java.util.ArrayList;
import java.util.Map;



public class PSRS {

    public static void main (String[] args) throws Exception{

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);

        //set streaming execution environment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //env.setParallelism(4);


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

        /**
         * Use Splitting and replication mechanism for partitioning
         */
        //TODO:change ni to be a paramter
        //DataStream<Tuple4<Integer,String,String,Float>> withKeyedStream = new SplittingAndReplication(inputStream).generateOneKey(inputStream,4);


        //*************************************** Recommendation part_IncNeigbr******************************************************************


        /*DataStream<Tuple3<Integer,String, Map<String, Float>>> estimatedRatesItems = new IncNeighbrCFRec().fit(
                withKeyedStream,10);
*/

        /*DataStream<Tuple3<Integer,String, Map<String,Float>>> estimatedRatesItems = new IncNeighbrCFRec().fit(
                withKeyedStream,10, "SlidingWindowUBCS"
        );*/

        /*DataStream<Tuple3<Integer,String, Map<String,Float>>> estimatedRatesItems = new IncNeighbrCFRec().fit(
                withKeyedStream,10,"LFU"
        );*/

        /*DataStream<Tuple3<Integer,String, Map<String,Float>>> estimatedRatesItems = new IncNeighbrCFRec().fit(
                withKeyedStream,10,"LRU"
        );*/

        //DataStream<Tuple3<Integer,String,ArrayList<String>>> recommendedItems = new IncNeighbrCFRec().recommend(estimatedRatesItems,10);

        //*************************************** Recommendation DISGD******************************************************************

        /*DataStream<Tuple3<String, String, Map<String, Float>>> estimatedRatesItems = new DISGD().fit2(
                withKeyedStream,10);*/

        /*DataStream<Tuple3<String, String, Map<String, Float>>> estimatedRatesItems = new DISGD().fit2(
                withKeyedStream,10,"LFU");*/


       // DataStream<Tuple3<String,String,ArrayList<String>>> recommendedItems = new DISGD().recommend2(estimatedRatesItems,10);

        //DataStream<Integer> recall = new RecallStream().recallStream2(recommendedItems);

        //*************************************** Recommendation TenRec******************************************************************
        /*DataStream<Tuple3<Integer, String, Map<String, Float>>> estimatedRatesItems = new TenRec().fit(
                withKeyedStream,10);*/


        DataStream<Tuple3<String,String,Map<String, Float>>> estimatedRatesItems = new TenRec().fit2(withKeyedStream,10,"LFU");

        //DataStream<Tuple3<Integer,String,ArrayList<String>>> recommendedItems = new TenRec().recommend(estimatedRatesItems,10);

        DataStream<Tuple3<String,String,ArrayList<String>>> recommendedItems = new TenRec().recommend2(estimatedRatesItems,10);

        DataStream<Integer> recall = new RecallStream().recallStream2(recommendedItems);


        //******************************************************************************************************************************


        //*************************************** Evaluation part******************************************************************

        //DataStream<Integer> recall = new RecallStream().recallStream(recommendedItems);


        //***************************************writing the output (sink)******************************************************************
        if(params.has("output")){
            recall.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE);
        }
        else {
            System.out.println("Use --output to specify file input ");
        }

        env.execute("TenRec ");
    }
}