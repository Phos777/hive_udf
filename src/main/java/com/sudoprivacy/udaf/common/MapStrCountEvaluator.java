package com.sudoprivacy.udaf.common;

import com.sudoprivacy.utils.UdfConvert;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("deprecation")
public class MapStrCountEvaluator extends GenericUDAFEvaluator {
    protected StandardMapObjectInspector combineOI;

    public static class MapAggregationBuffer implements AggregationBuffer {
        public Map<String, Integer> map;

        void combine(Map<String, Integer> otherMap) throws HiveException {
            if (null == otherMap || otherMap.isEmpty()) return;

            for (Map.Entry<?, ?> entry : otherMap.entrySet()) {
                String key = entry.getKey().toString();
                Integer value = UdfConvert.toInt(entry.getValue());

                if (map.containsKey(key)) {
                    map.put(key, map.get(key) + value);
                } else {
                    map.put(key, value);
                }
            }
        }
    }

    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
        MapAggregationBuffer mapAggregationBuffer = new MapAggregationBuffer();
        reset(mapAggregationBuffer);
        return mapAggregationBuffer;
    }

    public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
        ((MapAggregationBuffer) aggregationBuffer).map = new HashMap<String, Integer>();
    }

    public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
        throw new HiveException("Function iterate is not implemented!");
    }

    public void merge(AggregationBuffer aggregationBuffer, Object partial) throws HiveException {
        if (null != partial) {
            MapAggregationBuffer mapAggregationBuffer = (MapAggregationBuffer) aggregationBuffer;
            Map<String, Integer> partialMap = (Map<String, Integer>) combineOI.getMap(partial);
            mapAggregationBuffer.combine(partialMap);
        }
    }

    public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
        MapAggregationBuffer mapAggregationBuffer = (MapAggregationBuffer) aggregationBuffer;
        return mapAggregationBuffer.map;
    }

    public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
        throw new HiveException("Function terminate is not implemented!");
    }
}
