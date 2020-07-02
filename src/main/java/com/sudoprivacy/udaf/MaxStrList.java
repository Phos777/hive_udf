package com.sudoprivacy.udaf;


import com.sudoprivacy.utils.UdfConvert;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.lazy.LazyArray;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;

import java.util.*;

@Description(
        name = "max_strlist",
        value = "Return the most requent str in str lists.",
        extended = "Example:\n > SELECT max_strlist(col) from table;"
)
public class MaxStrList extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        return new MaxStrEvaluator();
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        if (1 != info.length) {
            throw new UDFArgumentLengthException("Function max_str takes 1 argument");
        }
        return new MaxStrEvaluator();
    }

    @SuppressWarnings("deprecation")
    public static class MaxStrEvaluator extends GenericUDAFEvaluator {

        private PrimitiveObjectInspector inputOI;
        private StandardMapObjectInspector combineOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            ObjectInspector returnKey = PrimitiveObjectInspectorFactory
                    .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
            ObjectInspector returnValue = PrimitiveObjectInspectorFactory
                    .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT);

            if (m == Mode.PARTIAL1) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
                return ObjectInspectorFactory.getStandardMapObjectInspector(returnKey, returnValue);
            } else if (m == Mode.PARTIAL2) {
                combineOI = (StandardMapObjectInspector) parameters[0];
                return ObjectInspectorFactory.getStandardMapObjectInspector(returnKey, returnValue);
            } else if (m == Mode.FINAL) {
                combineOI = (StandardMapObjectInspector) parameters[0];
                return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
            } else if (m == Mode.COMPLETE) {
                return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
            } else {
                throw new RuntimeException("No such mode");
            }
        }

        static class MapAggregationBuffer implements AggregationBuffer {
            Map<String, Integer> map;

            void combine(Map<String, Integer> otherMap) throws HiveException {
                if (null == otherMap || otherMap.isEmpty()) return;

                for (Map.Entry<?, ?> entry : otherMap.entrySet()) {
                    String key = UdfConvert.toStr(entry.getKey());
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

        public void putIntoMap(MapAggregationBuffer mapAggregationBuffer, String key) {
            if (mapAggregationBuffer.map.containsKey(key)) {
                mapAggregationBuffer.map.put(key, mapAggregationBuffer.map.get(key) + 1);
            } else {
                mapAggregationBuffer.map.put(key, 1);
            }
        }

        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            Object o = objects[0];
            if (o != null) {
                String[] keys = PrimitiveObjectInspectorUtils.getString(o, inputOI).split(",");
                List<String> array = Arrays.asList(keys);
                for (String str : array) {
                    putIntoMap((MapAggregationBuffer) aggregationBuffer, str);
                }
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
            Integer maxCount = 0;
            String maxStr = "";
            for (Map.Entry<String, Integer> entry : mapAggregationBuffer.map.entrySet()) {
                Integer currCount = UdfConvert.toInt(entry.getValue());
                if (currCount > maxCount) {
                    maxCount = currCount;
                    maxStr = entry.getKey();
                }
            }
            return new Text(maxStr);
        }
    }
}
