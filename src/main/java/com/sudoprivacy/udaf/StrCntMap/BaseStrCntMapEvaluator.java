package com.sudoprivacy.udaf.StrCntMap;

import com.sudoprivacy.enums.UdfDataType;
import com.sudoprivacy.enums.UdfOuputType;
import com.sudoprivacy.enums.UdfProcesType;
import com.sudoprivacy.utils.UdfConvert;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.lazy.LazyArray;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.*;

@SuppressWarnings("deprecation")
public class BaseStrCntMapEvaluator extends GenericUDAFEvaluator {
    protected PrimitiveObjectInspector inputOI;
    protected StandardMapObjectInspector combineOI;

    protected UdfDataType InputType() throws HiveException {
        throw new HiveException("InputType not specified");
    }

    protected UdfOuputType OutputType() throws HiveException {
        throw new HiveException("InputType not specified");
    }

    protected UdfProcesType ProcessType() throws HiveException {
        throw new HiveException("ProcessType not determined");
    }

    protected void putIntoMap(MapAggregationBuffer mapAggregationBuffer, String key) {
        if (mapAggregationBuffer.map.containsKey(key)) {
            mapAggregationBuffer.map.put(key, mapAggregationBuffer.map.get(key) + 1);
        } else {
            mapAggregationBuffer.map.put(key, 1);
        }
    }

    private ObjectInspector createDefaultMapInspector() {
        ObjectInspector returnKey = PrimitiveObjectInspectorFactory
                .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
        ObjectInspector returnValue = PrimitiveObjectInspectorFactory
                .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT);
        return ObjectInspectorFactory.getStandardMapObjectInspector(returnKey, returnValue);
    }

    private ObjectInspector createOutputInspector() throws HiveException {
        if (UdfOuputType.MapMaxCount == OutputType() || UdfOuputType.MapSum == OutputType()) {
            return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
        } else if (UdfOuputType.MapMax == OutputType()) {
            return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        } else {
            throw new HiveException(String.format("Not supported output type: {}", OutputType().toString()));
        }
    }

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
        super.init(m, parameters);

        if (m == Mode.PARTIAL1) {
            if (UdfDataType.ListAsStr == InputType() || UdfDataType.Str == InputType()) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
            }
            return createDefaultMapInspector();
        } else if (m == Mode.PARTIAL2) {
            combineOI = (StandardMapObjectInspector) parameters[0];
            return createDefaultMapInspector();
        } else if (m == Mode.FINAL) {
            combineOI = (StandardMapObjectInspector) parameters[0];
            return createOutputInspector();
        } else if (m == Mode.COMPLETE) {
            return createOutputInspector();
        } else {
            throw new RuntimeException("No such mode");
        }
    }

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
        Object o = objects[0];
        if (null == o) {
            return;
        }
        List<String> keys = new ArrayList<String>();
        if (UdfDataType.Str == InputType()) {
            keys.add(o.toString());
        } else if (UdfDataType.ListOfStr == InputType()) {
            LazyArray array = (LazyArray) o;
            for (int i = 0; i < array.getListLength(); i++) {
                keys.add(array.getListElementObject(i).toString());
            }
        } else if (UdfDataType.ListAsStr == InputType()) {
            keys = Arrays.asList(PrimitiveObjectInspectorUtils.getString(o, inputOI).split(","));
        } else {
            throw new HiveException(String.format("Unsupported input type: {}", InputType()));
        }

        MapAggregationBuffer mapAggregationBuffer = (MapAggregationBuffer) aggregationBuffer;
        switch (ProcessType()) {
            case Set:
                for (String key : new HashSet<String>(keys)) {
                    putIntoMap(mapAggregationBuffer, key);
                }
                break;
            case List:
                for (String key : keys) {
                    putIntoMap(mapAggregationBuffer, key);
                }
                break;
            default:
                throw new HiveException(String.format("Unsupported process type: {}", ProcessType()));
        }
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
        MapAggregationBuffer mapAggregationBuffer = (MapAggregationBuffer) aggregationBuffer;
        Integer sum = 0;
        Integer maxCount = 0;
        String maxStr = "";
        for (Map.Entry<String, Integer> entry : mapAggregationBuffer.map.entrySet()) {
            Integer currCount = UdfConvert.toInt(entry.getValue());
            if (currCount > maxCount) {
                maxCount = currCount;
                maxStr = entry.getKey();
            }
            sum += UdfConvert.toInt(entry.getValue());
        }

        switch (OutputType()) {
            case MapMax:
                return new Text(maxStr);
            case MapMaxCount:
                return new IntWritable(maxCount);
            case MapSum:
                return new IntWritable(sum);
            default:
                throw new HiveException(String.format("Unsupported output type: {}", OutputType()));
        }
    }
}
