package recommender;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class RecommenderUtilities {


    public ArrayList<Tuple2<String,Float>> getMostKuSimilar(Map<String,Float> toBeOrderedMap, Integer k){

        ArrayList<Tuple2<String,Float>> topKSimilar = new ArrayList<>();

        toBeOrderedMap.entrySet().stream().sorted(Map.Entry.comparingByValue())
                .limit(k)
                //thing,sim
                .forEach(thingSim -> topKSimilar.add(Tuple2.of(thingSim.getKey(), thingSim.getValue())));

        return topKSimilar;

    }

    public ArrayList<String>topKItems(Map<String,Float> toBeOrderedMap, Integer k){

        ArrayList<String> topKSimilar = new ArrayList<>();

        toBeOrderedMap.entrySet().stream().sorted(Map.Entry.comparingByValue())
                .limit(k)
                //thing,sim
                .forEach(thingSim -> topKSimilar.add(thingSim.getKey()));

        return topKSimilar;

    }


    public Map<String,Float> estimateRateForItems (MapState<String,Integer> allItems,ArrayList<Tuple2<String,Float>> topKSimilarthings,
                                                   MapState<String,Map<String,Float>> userItemRatingHistory) throws Exception {

        Map<String,Float> estimatedRatesForItems = new HashMap<>();


        for (String itemInAll : allItems.keys()){

            Float denominator = 0f;
            Float numerator = 0f;

            for (Tuple2<String, Float> simUser : topKSimilarthings ){

                Integer rateUserForItem;

                //estimated rate
                if(userItemRatingHistory.get(simUser.f0).containsKey(itemInAll)){

                    rateUserForItem = 1;
                }
                else {
                    rateUserForItem = 0;
                }

                //numerator
                numerator += rateUserForItem*(simUser.f1);

                //denominator
                denominator += simUser.f1;
            }

            Float estimatedRate = numerator/denominator;
            estimatedRatesForItems.put(itemInAll,estimatedRate);
        }

        return estimatedRatesForItems;
    }




}
