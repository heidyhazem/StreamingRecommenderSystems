package IncrementalNeighbourhood;


import org.apache.flink.api.java.tuple.Tuple3;

/**
 * This class contains function to organize the process of generting ordered pairs
 * to be ready for useing as unique Keys
 */

public class GeneratePairs {

    /**
     * convention of items pairs(min ID(FIRST),max ID(Second)) sorting to avoid any duplicates.
     * and generating unique keys
     * @param currentId1 one of the ids
     * @param id2 one of the  ids
     * @return pair of ordered ids to be a key with the position of the current id
     */

    public Tuple3<String,String,Integer> getKey (String currentId1, String id2){

        Integer position;

        String firstItemInPair = String.valueOf(Math.min(Long.valueOf(currentId1),Long.valueOf(id2)));
        String secondItemInPair = String.valueOf(Math.max(Long.valueOf(currentId1),Long.valueOf(id2)));

        if(currentId1.equals(firstItemInPair)){
            position = 0;
        }
        else{
            position = 1;
        }


        return Tuple3.of(firstItemInPair,secondItemInPair, position);
    }


}
