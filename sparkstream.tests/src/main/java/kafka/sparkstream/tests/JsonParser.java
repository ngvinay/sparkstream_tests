package kafka.sparkstream.tests;

import java.io.IOException;

import org.apache.log4j.Logger;

import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

public class JsonParser
    implements PairFunction<String, Long, String>
{
    private static final long serialVersionUID = 42l;
    private final ObjectMapper mapper = new ObjectMapper();

    //@Override
    public Tuple2<Long, String> call(String tweet)
    {
        try
        {
            JsonNode root = mapper.readValue(tweet, JsonNode.class);
            //long id;
            long pop;
            String city;
            //if (root.get("lang") != null &&
            //    "en".equals(root.get("lang").textValue()))
            //{
                if (root.get("pop") != null && root.get("city") != null)
                {
                    pop = root.get("pop").longValue();
                    city = root.get("city").textValue();
                    return new Tuple2<Long, String>(pop, city);
                }
                return null;
            //}
            //return null;
        }
        catch (IOException ex)
        {
            Logger LOG = Logger.getLogger(this.getClass());
            LOG.error("IO error while filtering tweets", ex);
            LOG.trace(null, ex);
        }
        return null;
    }
}

