package Recommender;

import Forgetting.LFU_DISGD;
import Forgetting.LFU_TenRec;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.primitives.Floats.min;

public class TenRec extends RecommenderAbstract {

    @Override
    public DataStream<Tuple3<Integer, String, Map<String, Float>>> fit(DataStream<Tuple4<Integer, String, String, Float>> withKeyStream, Integer k) {


        DataStream<Tuple3<Integer, String, Map<String, Float>>> itemsScores = withKeyStream.
                keyBy(0)
                .process(new KeyedProcessFunction<Tuple, Tuple4<Integer, String, String, Float>, Tuple3<Integer, String, Map<String, Float>>>() {

                    MapState<String,Map<String,Float>> userItemRatingHistory;
                    MapState<Tuple2<String,String>,Float> pairItemCoRating;
                    MapState<String,Long> itemCount;

                    String user;
                    String item;
                    Float rate;


                    @Override
                    public void processElement(Tuple4<Integer, String, String, Float> input, Context context, Collector<Tuple3<Integer, String, Map<String, Float>>> out) throws Exception {


                        user = input.f1;
                        item = input.f2;
                        rate = input.f3;




                        if(userItemRatingHistory.contains(user)){
                            for(Map.Entry<String,Float> ratedItemByCurrentUser : userItemRatingHistory.get(user).entrySet()){
                                //to calculate deltacorating between current item and item rated by user before
                                Float deltaCoRating = min(rate,ratedItemByCurrentUser.getValue());
                                //convention of items pairs(min ID(FIRST),max ID(Second)) guarantee sorting to avoid any duplicates
                                String firstItemInPair = String.valueOf(Math.min(Long.valueOf(item),Long.valueOf(ratedItemByCurrentUser.getKey())));
                                String secondItemInPair = String.valueOf(Math.max(Long.valueOf(item),Long.valueOf(ratedItemByCurrentUser.getKey())));

                                if(pairItemCoRating.contains(Tuple2.of(firstItemInPair,secondItemInPair))){
                                    Float oldCoRating = pairItemCoRating.get(Tuple2.of(firstItemInPair,secondItemInPair));
                                    pairItemCoRating.put(Tuple2.of(firstItemInPair,secondItemInPair),oldCoRating+deltaCoRating);
                                }
                                else {
                                    pairItemCoRating.put(Tuple2.of(firstItemInPair,secondItemInPair),deltaCoRating);
                                }
                            }
                        }

                        else{
                            //user is not known so no history hence no update for the pairs
                            //and no co rating change§§
                            Map<String,Float> newitemRateMap = new HashMap<String, Float>();
                            newitemRateMap.put(item,rate);
                            userItemRatingHistory.put(user,newitemRateMap);
                        }


                        //++++++++++++++++++++++++++++++item count+++++++++++++++++++++++++++++++++++++++++++

                        //Update item Count
                        if(itemCount.contains(item)){
                            Long newCount = itemCount.get(item)+rate.longValue();
                            //itemCount.remove(item);
                            itemCount.put(item,newCount);
                        }
                        else {
                            itemCount.put(item,rate.longValue());
                        }

                        //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                        //******************************************************************************************************************
                        //all items

                        Iterable<String> AllItems = itemCount.keys();
                        Map<String,Float> allRelatedItemsToTheItem = new HashMap<>();
                        ArrayList<Tuple2<String,Float>> mostSimilarNItems = new ArrayList<>();

                        //mn ela5r hna byro7 ygeb kol el items eli m7soblha sim m3 el item
                        for(String itemFromAll : AllItems){
                            String firstItemInPairinternal = String.valueOf(Math.min(Long.valueOf(item),Long.valueOf(itemFromAll)));
                            String secondItemInPairinternal = String.valueOf(Math.max(Long.valueOf(item),Long.valueOf(itemFromAll)));
                            if (pairItemCoRating.contains(Tuple2.of(firstItemInPairinternal,secondItemInPairinternal))){
                                allRelatedItemsToTheItem.put(itemFromAll,pairItemCoRating.get(Tuple2.of(firstItemInPairinternal,secondItemInPairinternal)));
                            }
                            else {
                                continue;
                            }
                        }


                        allRelatedItemsToTheItem.entrySet().stream().sorted(Map.Entry.comparingByValue())
                                .limit(k)
                                .forEach(itemRate -> mostSimilarNItems.add(Tuple2.of(itemRate.getKey(),itemRate.getValue())));
                        //***************************************************************************************************************

                        //************************************************ 1- recommend Estimate Rate for unseen data****************************************

                        Map<String,Float> estimatedRatesOfItems = new HashMap<>();



                        for(String itemFromAll : AllItems){
                            Float estRate;

                            // itemRateMap rated items by user is the rated items by the current user not including the one he just rate
                            //make sure the new item is not included for guarantee score
                            //itemRateMap :rated item by user
                            if(userItemRatingHistory.get(user).containsKey(itemFromAll)){
                                //the item is rated before no need to give an estimated rate
                                continue;
                            }
                            else{
                                //get top K similar items to the current item from items rated by the user
                                ArrayList<Tuple3<String,Float, Float>> mostKSimilarItems;
                                //mostKSimilarItems = Similarities.getToKSimilarUserBased(pairItemCoRating,itemRateMap,item,k);
                                mostKSimilarItems = RecommenderUtilities.getToKSimilarUserBased(pairItemCoRating,userItemRatingHistory.get(user),item,k);
                                estRate = RecommenderUtilities.estimatRate_TenRec(mostKSimilarItems);
                                estimatedRatesOfItems.put(itemFromAll,estRate);

                            }
                        }

                        out.collect(Tuple3.of(input.f0,item,estimatedRatesOfItems));


                        ////**********************************************************************************************************************************

                        //************************************************ 3- Update the model ****************************************

                        //updated rated items list( )
                        if(userItemRatingHistory.contains(user)){

                            Map<String,Float> toupdate = userItemRatingHistory.get(user);
                            toupdate.put(item,rate);
                            userItemRatingHistory.put(user,toupdate);

                        }
                        else {
                            Map<String,Float> toupdate = new HashMap<>();
                            toupdate.put(item,rate);
                            userItemRatingHistory.put(user,toupdate);

                        }
                    }


                    @Override
                    public void open(Configuration config) {


                        MapStateDescriptor<Tuple2<String, String>, Float> descriptor1 =
                                new MapStateDescriptor<>(
                                        "userItemRatingHistory",
                                        TypeInformation.of(new TypeHint<Tuple2<String, String>>() {

                                        }),
                                        TypeInformation.of(new TypeHint<Float>() {

                                        })
                                );

                        MapStateDescriptor<Tuple2<String, String>, Float> descriptor2 =
                                new MapStateDescriptor<>(
                                        "PairItemsCorating",
                                        TypeInformation.of(new TypeHint<Tuple2<String, String>>() {

                                        }),
                                        TypeInformation.of(new TypeHint<Float>() {

                                        })
                                );

                        MapStateDescriptor<String, Long> descriptor3 =
                                new MapStateDescriptor<>(
                                        "itemCount",
                                        TypeInformation.of(new TypeHint<String>() {

                                        }),
                                        TypeInformation.of(new TypeHint<Long>() {

                                        })
                                );

                        MapStateDescriptor<String, Map<String, Float>> descriptor4 =
                                new MapStateDescriptor<>(
                                        "userItemRatingHistory",
                                        TypeInformation.of(new TypeHint<String>() {

                                        }),
                                        TypeInformation.of(new TypeHint<Map<String, Float>>() {

                                        })
                                );

                        userItemRatingHistory = getRuntimeContext().getMapState(descriptor4);
                        pairItemCoRating = getRuntimeContext().getMapState(descriptor2);
                        itemCount = getRuntimeContext().getMapState(descriptor3);

                    }



                });

        return itemsScores;
    }

    @Override
    public DataStream<Tuple3<Integer, String, Map<String, Float>>> fit(DataStream<Tuple4<Integer, String, String, Float>> withKeyStream, Integer k, String forgettingTechnique) {
        return null;
    }



    public DataStream<Tuple3<String, String, Map<String, Float>>> fit2(DataStream<Tuple4<Integer, String, String, Float>> withKeyStream, Integer k, String forgettingTechnique) {

        if(forgettingTechnique.equals("LFU")){
            return new LFU_TenRec().fit2(withKeyStream,10,"LFU");
        }
        else{
            return null;
        }

    }

    @Override
    public DataStream<Tuple3<Integer, String, ArrayList<String>>> recommend(DataStream<Tuple3<Integer, String, Map<String, Float>>> estimatedRatesOfItems, Integer k) {
        return  new RecommenderUtilities().recommend_Top_N(estimatedRatesOfItems,k);
    }

    public DataStream<Tuple3<String, String, ArrayList<String>>> recommend2(DataStream<Tuple3<String, String, Map<String, Float>>> estimatedRatesOfItems, Integer k) {
        return  new RecommenderUtilities().recommend_Top_N2(estimatedRatesOfItems,k);
    }
}