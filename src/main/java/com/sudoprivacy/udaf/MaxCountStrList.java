package com.sudoprivacy.udaf;


import com.sudoprivacy.udaf.common.MapStrCountEvaluator;
import com.sudoprivacy.utils.UdfConvert;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;

import java.util.*;

@Description(
        name = "max_cnt_strlist",
        value = "Return  count of the most frequent str in str lists.",
        extended = "Example:\n > SELECT max_cnt_strlist(col) from table;"
)
public class MaxCountStrList extends AbstractGenericUDAFResolver {
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
    public static class MaxStrEvaluator extends MapStrCountEvaluator {
        private PrimitiveObjectInspector inputOI;

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
                return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
            } else if (m == Mode.COMPLETE) {
                return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
            } else {
                throw new RuntimeException("No such mode");
            }
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

        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            MapAggregationBuffer mapAggregationBuffer = (MapAggregationBuffer) aggregationBuffer;
            Integer maxCount = 0;
            for (Map.Entry<String, Integer> entry : mapAggregationBuffer.map.entrySet()) {
                Integer currCount = UdfConvert.toInt(entry.getValue());
                if (currCount > maxCount) {
                    maxCount = currCount;
                }
            }
            return new IntWritable(maxCount);
        }
    }
}