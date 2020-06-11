package recommender;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.ArrayList;
import java.util.Map;

public class IncNeighbrCFRec extends recommenderAbstract{

    private DataStream<Tuple4<Integer, String, String, Float>> withKeyStream;

    /**
     Constructs the input KeyedStream
     @param withKeyStream the input KeyedStream
     */
    public void IncNeighbrCFRec(DataStream<Tuple4<Integer, String, String, Float>> withKeyStream){
        withKeyStream = this.withKeyStream;
    }


    @Override
    public Map<String, Float> fit(DataStream<Tuple4<Integer, String, String, Float>> withKeyStream) {
        return null;
    }

    @Override
    public ArrayList<String> recommend(Map<String, Float> estimatedRatesOfItems) {
        return null;
    }
}
