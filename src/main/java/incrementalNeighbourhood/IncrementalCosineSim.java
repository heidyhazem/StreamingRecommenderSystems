/**
 * Most state-of-the-art Collaborative Filtering (CF) algorithms are based on either neighborhood methods or matrix factorization.
 * Fundamentally, these differ on the strategy used to process user feedback. This user feedback can be conceptually seen as a user-item matrix,
 * known in most literature as the ratings matrix, in which cells contain information about user preferences over items.
 */

package incrementalNeighbourhood;


/**
 * For implementing  collaborative filtering algorithm based on Cosine Similarity neighbourhood method
 * The Incremental Cosine similarity here assuming binary data(ratings).
 */
public class IncrementalCosineSim {

    /**
     * Calculate cosine similarity for incrementally based on equation|(Iu∩Iv)|/|Iu|× |Iv|
     * @param commonCount is the count of common items rated by both of users
     * @param count1 is the count of items rated by the first user
     * @param count2 is the count of items rated by the second user
     * @return the cosine similarity between two users
     */
    public Float calculatecosineSimilarity (Integer commonCount, Integer count1, Integer count2){
        Float similarity = (float)commonCount/(count1*count2);
        return similarity;
    }
}
