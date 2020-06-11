package recommender;


import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.ArrayList;
import java.util.Map;

/**
represents how to implement recommender algorithm
 */
public abstract class recommenderAbstract {


    /**
     *To recommend recommendation list for the user
     * @param withKeyStream The input data stream Keyed for partitioning(key,user,item,rate)
     * @return Map of predicted rates for the items  user has not rated
     */
    public abstract Map<String,Float> fit(DataStream<Tuple4<Integer,String,String,Float>> withKeyStream);




    /**
     *To recommend recommendation list for the user
     * @param estimatedRatesOfItems The predicted rates for the items  user has not rated
     * @return ArrayList of the recommended items
     */
    public abstract ArrayList<String> recommend(Map<String,Float> estimatedRatesOfItems);



}
