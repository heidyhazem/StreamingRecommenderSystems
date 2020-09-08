package Forgetting;

import incrementalNeighbourhood.GeneratePairs;
import incrementalNeighbourhood.IncrementalCosineSim;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.shaded.org.joda.time.DateTime;
import org.apache.flink.table.shaded.org.joda.time.Days;
import org.apache.flink.util.Collector;
import recommender.RecommenderUtilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static sun.java2d.xr.XRUtils.None;

//TODO: Take k a a variable
/**
 * The implementation of the ProcessFunction that maintains the count and timeouts
 */
public class LRUForgetting
        extends KeyedProcessFunction<Tuple, Tuple4<Integer, String, String, Float>, Tuple3<Integer, String, Map<String, Float>>> {
    /**
     * holds state for user's rating for items as we need to know which items the user rated to
     * calculate the user's similarities
     */
    private MapState<String, Map<String, Float>> userItemRatingHistory;
    /**
     * holds state for user's similarities parts to be incrementally updated((user,user),countOfCommonItems,countOfRatedItemsU1,countOfRatedItemsU2)
     */
    private MapState<Tuple2<String, String>, Tuple4<Integer, Integer, Integer, Float>> userSimilarities;
    /**
     * holds all items in the system used to get estimated rating for unrated items for the users
     */
    private MapState<String, Integer> allItems;
    /**
     * holds whether timeStamp or count
     */
    private MapState<String, Long> userFootPrint;
    private MapState<String, Long> itemFootPrint;
    private ValueState<Long> generalCounter;
    private ValueState<Boolean> status;

    Integer k = 10;



    @Override
    public void processElement(Tuple4<Integer, String, String, Float> input, Context context, Collector<Tuple3<Integer, String, Map<String, Float>>> out) throws Exception {

        String user = input.f1;
        String item = input.f2;
        Float rate = input.f3;


        if (userItemRatingHistory.contains(user)) {
            //prepare Map<user,sim>
            Map<String, Float> currentUserSimilaritiesMap = new HashMap<>();

            //###################### get top k similar user#######################################################

            //generate pairs
            for (String userInHistory : userItemRatingHistory.keys()) {
                //get the ordered pairOf users(key)
                Tuple3<String, String, Integer> keyWithPosition = new GeneratePairs().getKey(user, userInHistory);
                Tuple2<String, String> userPair = Tuple2.of(keyWithPosition.f0, keyWithPosition.f1);
                //Integer positionOfCurrentUserInTheTuple = keyWithPosition.f2;

                try {

                    Float sim = userSimilarities.get(userPair).f3;

                    currentUserSimilaritiesMap.put(userInHistory, sim);

                } catch (Exception e) {
                    System.out.println("The pair dees not exist");
                }
            }

            //method to get top k users
            ArrayList<Tuple2<String, Float>> topKSimilarUsers = new RecommenderUtilities().getMostKuSimilar(currentUserSimilaritiesMap, k);

            //step2: Estimate rate from user to all unrated items

            Map<String, Float> estimatedRatesForItems;

            estimatedRatesForItems = new RecommenderUtilities().estimateRateForItems(
                    allItems, topKSimilarUsers, userItemRatingHistory, user);

            //htl3 el map de ashan ytrtbo we ytl3 recommendation
            out.collect(Tuple3.of(input.f0, item, estimatedRatesForItems));
        }

        //_________________________________________________________________________________________________________________________________
        //_________________________________________________________________________________________________________________________________
        //Forgetting
        //As per documentation it retrieves event timestamp

        if(status.value()){
            //TODO: Remove this state
            //if the first record only
            generalCounter.update(context.timestamp());
            status.update(false);
        }



        Map<String,Float> output2 = new HashMap<>();
        output2.put("hhh",99999999f);

        //out.collect(Tuple3.of(777777777,"Heidy",output2));

        //out.collect(Tuple3.of(Integer.valueOf(Math.toIntExact(context.timestamp())),"timeStampCurrent",output2));
        //out.collect(Tuple3.of(Math.toIntExact(generalCounter.value()),"timeStampRegistwred",output2));


        if(context.timestamp() >= generalCounter.value() + 4640482){



            Map<String,Float> output = new HashMap<>();
            output.put("hhh",99999999f);

            //out.collect(Tuple3.of(55555555,"Heidy",output));

            DateTime end   = new DateTime(context.timestamp());

            generalCounter.update(context.timestamp());


            Map<String, Long> userFootPrintClone = new HashMap<>();
            for(Map.Entry<String,Long> userMap : userFootPrint.entries()){
                userFootPrintClone.put(userMap.getKey(),userMap.getValue());
            }

            for(Map.Entry<String,Long> userMap : userFootPrintClone.entrySet()){

                /*DateTime start = new DateTime(userMap.getValue());
                Days days = Days.daysBetween(start, end);
                int user_idle_Days = days.getDays();*/


                //forget users
                if(context.timestamp() >=  userMap.getValue()+4640482 ){

                    //out.collect(Tuple3.of(777777777,"Heidy",output));
                    userFootPrint.remove(userMap.getKey());
                    userItemRatingHistory.remove(userMap.getKey());

                    //TODO:remove from similarities and it can be optimised when using matrix
                    for (String userInHistory : userItemRatingHistory.keys()) {
                        //get the ordered pairOf users(key)
                        Tuple3<String, String, Integer> keyWithPosition = new GeneratePairs().getKey(userMap.getKey(), userInHistory);
                        Tuple2<String, String> userPair = Tuple2.of(keyWithPosition.f0, keyWithPosition.f1);
                        //Integer positionOfCurrentUserInTheTuple = keyWithPosition.f2;

                        //Step 2 : remove Pair from user's footprint history state
                        userSimilarities.remove(userPair);
                    }
                }
            }

            Map<String, Long> itemFootPrintClone = new HashMap<>();
            for(Map.Entry<String,Long> itemMap : itemFootPrint.entries()){
                itemFootPrintClone.put(itemMap.getKey(),itemMap.getValue());
            }

            for(Map.Entry<String,Long> itemMap : itemFootPrintClone.entrySet()){

                /*DateTime start = new DateTime(itemMap.getValue());
                Days days = Days.daysBetween(start, end);
                int item_idle_Days = days.getDays();*/



                //forget items
                if(context.timestamp() >=  itemMap.getValue()  + 4640482){

                    //out.collect(Tuple3.of(66666666,"Heidy",output));

                    allItems.remove(itemMap.getKey());
                    itemFootPrint.remove(itemMap.getKey());
                }

            }

        }

        userFootPrint.put(user,context.timestamp());
        itemFootPrint.put(item,context.timestamp());

        // schedule the next timer 60 seconds from the current event time
        //context.timerService().registerEventTimeTimer((long) (generalCounter.value() + 4.32e+9));




        //_________________________________________________________________________________________________________________________________
        //_________________________________________________________________________________________________________________________________
        //update similarities
        if (userItemRatingHistory.contains(user)) {
            /**
             * Updating similarities state
             */
            //generate pairs
            //step 2:1 similarities
            for (String userInHistory : userItemRatingHistory.keys()) {

                //for not generating tuple with the same user
                if (!user.equals(userInHistory)) {

                    //get the ordered pairOf users(key)
                    Tuple3<String, String, Integer> keyWithPosition = new GeneratePairs().getKey(user, userInHistory);
                    Tuple2<String, String> userPair = Tuple2.of(keyWithPosition.f0, keyWithPosition.f1);
                    Integer positionOfCurrentUserInTheTuple = keyWithPosition.f2;

                    //Step 2:2 : get Pair from state to update it incrementally
                    Tuple4<Integer, Integer, Integer, Float> commonCount1Count2 = userSimilarities.get(userPair);


                    //step 2:3 : check if the the other user like this item before or not
                    //to update common Count (first number in the tuple) or not

                    Integer commonCount = commonCount1Count2.f0;
                    if (userItemRatingHistory.get(userInHistory).containsKey(item)) {
                        commonCount1Count2.setField(commonCount + 1, 0);
                    }

                    //step2:4 : increment the current user count of items
                    Integer currentUserItemCount = commonCount1Count2.getField(positionOfCurrentUserInTheTuple + 1);
                    commonCount1Count2.setField(currentUserItemCount + 1, positionOfCurrentUserInTheTuple + 1);


                    //userSimilarities
                    Float cosSim = new IncrementalCosineSim().calculatecosineSimilarity(commonCount1Count2.f0, commonCount1Count2.f1, commonCount1Count2.f2);

                    commonCount1Count2.setField(cosSim, 3);

                    //step2:5 update sim state itself
                    userSimilarities.put(userPair, commonCount1Count2);
                }

            }

            //******************************************************************************************************
            //********************** Step3: Update other states  **************************************************
            // userItemRatingHistory

            Map<String, Float> toUpdateItems = userItemRatingHistory.get(user);

            toUpdateItems.put(item, rate);
            userItemRatingHistory.put(user, toUpdateItems);

            //allItems
            if (!allItems.contains(item)) {
                allItems.put(item, None);
            }
            //******************************************************************************************************


        }
        //user is not known
        else {

            Integer countCurrentUser = 1;
            for (String userInHistory : userItemRatingHistory.keys()) {

                //Step 2:2 : Initialize similarity parts
                Tuple4<Integer, Integer, Integer, Float> commonCount1Count2 = Tuple4.of(0, 0, 0, 0f);

                //get the ordered pairOf users(key)
                Tuple3<String, String, Integer> keyWithPosition = new GeneratePairs().getKey(user, userInHistory);
                Tuple2<String, String> userPair = Tuple2.of(keyWithPosition.f0, keyWithPosition.f1);
                Integer positionOfCurrentUserInTheTuple = keyWithPosition.f2 + 1;

                //commonCount
                if (userItemRatingHistory.get(userInHistory).containsKey(item)) {
                    commonCount1Count2.setField(1, 0);
                }

                //CurrentUserCount new user so the count of items rated by user is one
                commonCount1Count2.setField(1, positionOfCurrentUserInTheTuple);

                //otherUserItemsCount
                Integer otherUserItemsCount = userItemRatingHistory.get(userInHistory).size();

                //get the position of the other item in pair
                Integer positionOfTheOtherUser;
                if (positionOfCurrentUserInTheTuple.equals(1)) {
                    positionOfTheOtherUser = 2;
                } else {
                    positionOfTheOtherUser = 1;
                }

                commonCount1Count2.setField(otherUserItemsCount, positionOfTheOtherUser);

                //userSimilarities
                Float cosSim = new IncrementalCosineSim().calculatecosineSimilarity(commonCount1Count2.f0, commonCount1Count2.f1, commonCount1Count2.f2);

                Map<String, Float> y = new HashMap<>();
                y.put("item", Float.valueOf(cosSim));
                //out.collect(Tuple3.of(999,userInHistory,y));

                commonCount1Count2.setField(cosSim, 3);
                userSimilarities.put(userPair, commonCount1Count2);

            }

            //********************** Step3: Update other states  **************************************************

            // userItemRatingHistory
            Map<String, Float> toUpdateItems = new HashMap<>();


            toUpdateItems.put(item, rate);

            userItemRatingHistory.put(user, toUpdateItems);

            //allItems
            if (!allItems.contains(item)) {
                allItems.put(item, None);
            }
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        MapStateDescriptor<Tuple2<String, String>, Tuple4<Integer, Integer, Integer, Float>> descriptor2 =
                new MapStateDescriptor<>(
                        "UserUserSimilarities",
                        TypeInformation.of(new TypeHint<Tuple2<String, String>>() {

                        }),
                        TypeInformation.of(new TypeHint<Tuple4<Integer, Integer, Integer, Float>>() {

                        })
                );

        MapStateDescriptor<String, Integer> descriptor3 =
                new MapStateDescriptor<>(
                        "allItems",
                        TypeInformation.of(new TypeHint<String>() {

                        }),
                        TypeInformation.of(new TypeHint<Integer>() {

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

        MapStateDescriptor<String, Long> desriptor5 =
                new MapStateDescriptor<String, Long>(
                        "user(count)",
                        TypeInformation.of(new TypeHint<String>() {
                        }),
                        TypeInformation.of(new TypeHint<Long>() {
                        })

                );
        MapStateDescriptor<String, Long> desriptor6 =
                new MapStateDescriptor<String, Long>(
                        "item(count)",
                        TypeInformation.of(new TypeHint<String>() {
                        }),
                        TypeInformation.of(new TypeHint<Long>() {
                        })

                );

        ValueStateDescriptor<Long> descriptor7 =
                new ValueStateDescriptor<Long>(
                        "generalCounter",
                        Long.class,
                        0L
                );

        ValueStateDescriptor<Boolean> descriptor8 =
                new ValueStateDescriptor<Boolean>(
                        "status",
                        Boolean.class,
                        true
                );

        userItemRatingHistory = getRuntimeContext().getMapState(descriptor4);
        userSimilarities = getRuntimeContext().getMapState(descriptor2);
        allItems = getRuntimeContext().getMapState(descriptor3);
        userFootPrint = getRuntimeContext().getMapState(desriptor5);
        itemFootPrint = getRuntimeContext().getMapState(desriptor6);
        generalCounter = getRuntimeContext().getState(descriptor7);
        status = getRuntimeContext().getState(descriptor8);


    }

    /*@Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<Tuple3<Integer, String, Map<String, Float>>> out) throws Exception {

        Long lastmodifiedTimeStamp = generalCounter.value();
        DateTime end   = new DateTime(timestamp);

        //50 days in miiliseconds
        if(timestamp >= lastmodifiedTimeStamp + 4.32e+9){
        //30 days
        *//*if(timestamp >= lastmodifiedTimeStamp + 8.64e+8){*//*

            Map<String,Float> output = new HashMap<>();
            output.put("hhh",99999999f);

            out.collect(Tuple3.of(9999,"Heidy",output));

            Map<String, Long> userFootPrintClone = new HashMap<>();
            userFootPrint.putAll(userFootPrintClone);

            for(Map.Entry<String,Long> userMap : userFootPrintClone.entrySet()){

                DateTime start = new DateTime(userMap.getValue());
                Days days = Days.daysBetween(start, end);
                int user_idle_Days = days.getDays();

                //forget users
                if(user_idle_Days > 3){

                    out.collect(Tuple3.of(777777777,"Heidy",output));
                    userFootPrint.remove(userMap.getKey());
                    userItemRatingHistory.remove(userMap.getKey());

                    //TODO:remove from similarities and it can be optimised when using matrix
                    for (String userInHistory : userItemRatingHistory.keys()) {
                        //get the ordered pairOf users(key)
                        Tuple3<String, String, Integer> keyWithPosition = new GeneratePairs().getKey(userMap.getKey(), userInHistory);
                        Tuple2<String, String> userPair = Tuple2.of(keyWithPosition.f0, keyWithPosition.f1);
                        //Integer positionOfCurrentUserInTheTuple = keyWithPosition.f2;

                        //Step 2 : remove Pair from user's footprint history state
                        userSimilarities.remove(userPair);
                    }
                }
            }

            Map<String, Long> itemFootPrintClone = new HashMap<>();
            itemFootPrint.putAll(itemFootPrintClone);

            for(Map.Entry<String,Long> itemMap : userFootPrintClone.entrySet()){

                DateTime start = new DateTime(itemMap.getValue());
                Days days = Days.daysBetween(start, end);
                int item_idle_Days = days.getDays();

                //forget items
                if(item_idle_Days > 3){
                    out.collect(Tuple3.of(66666666,"Heidy",output));
                    allItems.remove(itemMap.getKey());
                    itemFootPrint.remove(itemMap.getKey());
                }

            }

            generalCounter.update(timestamp);





        }
    }*/
}