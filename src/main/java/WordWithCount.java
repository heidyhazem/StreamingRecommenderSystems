import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.time.Instant;

/*public class WordWithCount {

    public String word;
    public int count;

    public WordWithCount() {}

    public WordWithCount(String word, int count) {
        this.word = word;
        this.count = count;
    }
}*/

public class WordWithCount {
    private String name;
    public Instant specialMoment;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


    public static <T> TypeInformation<T> WordWithCount(Class<T> WordWithCount) {
        final TypeInformation<T> ti = TypeExtractor.createTypeInfo(WordWithCount);
        if (ti instanceof PojoTypeInfo) {
            return ti;
        }
        throw new InvalidTypesException("POJO type expected but was: " + ti);
    }


}