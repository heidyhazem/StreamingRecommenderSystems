package recommender;

import Matrix.SparseBinaryMatrix;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RecommenderUtilities {

    /**
     *
     * @param toBeOrderedMap Map of string as key and float as score to be ordered and retrieve most top k
     * @param k how many items/users to be retrieved as most similar
     * @return top k similar users/items
     */

    public ArrayList<Tuple2<String,Float>> getMostKuSimilar(Map<String,Float> toBeOrderedMap, Integer k){

        ArrayList<Tuple2<String,Float>> topKSimilar = new ArrayList<>();

        toBeOrderedMap.entrySet().stream().sorted(Map.Entry.comparingByValue())
                .limit(k)
                //thing,sim
                .forEach(thingSim -> topKSimilar.add(Tuple2.of(thingSim.getKey(), thingSim.getValue())));

        return topKSimilar;

    }

    /**
     *
     * @param toBeOrderedMap Map of items and estimated rates
     * @param k howa many items to be recommended
     * @return top similar items
     */
    public ArrayList<String>topKItems(Map<String,Float> toBeOrderedMap, Integer k){

        ArrayList<String> topKSimilar = new ArrayList<>();

        toBeOrderedMap.entrySet().stream().sorted(Map.Entry.comparingByValue())
                .limit(k)
                //thing,sim
                .forEach(thingSim -> topKSimilar.add(thingSim.getKey()));

        return topKSimilar;

    }

    /**
     *
     * @param allItems all items
     * @param topKSimilarthings top k similar items/users
     * @param userItemRatingHistory Map of user and his/her rated itesm
     * @return  Map of item and the corresponding estimated rate
     * @throws Exception
     */
    public Map<String,Float> estimateRateForItems (MapState<String,Integer> allItems,ArrayList<Tuple2<String,Float>> topKSimilarthings,
                                                   MapState<String,Map<String,Float>> userItemRatingHistory, String curentUser) throws Exception {

        Map<String,Float> estimatedRatesForItems = new HashMap<>();


        for (String itemInAll : allItems.keys()){

            //skipping already rated items by the user
            if( ! userItemRatingHistory.get(curentUser).containsKey(itemInAll)){
                //TODO: Handle Nulls
                Float denominator = 0f;
                Float numerator = 0f;

                for (Tuple2<String, Float> simUser : topKSimilarthings ){

                    Integer rateUserForItem;

                    //estimated rate
                    //to know what the rate for the other user to the item
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

        }

        return estimatedRatesForItems;
    }



    public Map<String,Float> estimateRateForItems (Set<String> allItems, ArrayList<Tuple2<String,Float>> topKSimilarthings,
                                                   SparseBinaryMatrix userItemRatingHistory, String curentUser) throws Exception {


        Map<String,Float> estimatedRatesForItems = new HashMap<>();

        for (String itemInAll : allItems){
            if( ! userItemRatingHistory.ratedPositively(curentUser,itemInAll)){
                //TODO: Handle Nulls
                Float denominator = 0f;
                Float numerator = 0f;

                for (Tuple2<String, Float> simUser : topKSimilarthings ){
                    Integer rateUserForItem;

                    //estimated rate
                    //to know what the rate for the other user to the item

                    if(userItemRatingHistory.ratedPositively(simUser.f0,itemInAll)){
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
        }

        return estimatedRatesForItems;
    }
}
