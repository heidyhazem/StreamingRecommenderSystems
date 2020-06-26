package recommender;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.ArrayList;
import java.util.Map;

/**
represents how to implement recommender algorithm
 */
public abstract class RecommenderAbstract {


    /**
     *To recommend recommendation list for the user
     * @param withKeyStream The input data stream Keyed for partitioning(key,user,item,rate)
     * @param k  count of similar items/users
     * @return Map of predicted rates for the items  user has not rated
     */
    public abstract DataStream<Tuple3<Integer,String,Map<String,Float>>> fit(DataStream<Tuple4<Integer,String,String,Float>> withKeyStream, Integer k);




    /**
     *To recommend recommendation list for the user
     * @param estimatedRatesOfItems The predicted rates for the items  user has not rated
     * @param k number ot recommended items
     * @return ArrayList of the recommended items
     */
    public abstract DataStream<Tuple3<Integer,String,ArrayList<String>>> recommend(DataStream<Tuple3<Integer,String,Map<String,Float>>> estimatedRatesOfItems,Integer k);



}
