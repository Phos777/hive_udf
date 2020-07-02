package com.sudoprivacy.udaf;


import com.sudoprivacy.utils.UdfConvert;
import org.apache.commons.lang.StringUtils;
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
import org.apache.hadoop.io.Text;

import java.util.*;

@Description(
        name = "any_str",
        value = "Run map-reduce and get the last element.",
        extended = "Example:\n > SELECT any_str(col) from table;"
)
public class AnyStr extends AbstractGenericUDAFResolver {
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

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            inputOI = (PrimitiveObjectInspector) parameters[0];
            return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        }

        static class StrAggregationBuffer implements AggregationBuffer {
            String res;

            void reduce(String otherStr) throws HiveException {
                if (StringUtils.isEmpty(otherStr)) return;
                res = otherStr;
            }
        }

        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            StrAggregationBuffer mapAggregationBuffer = new StrAggregationBuffer();
            reset(mapAggregationBuffer);
            return mapAggregationBuffer;
        }

        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            ((StrAggregationBuffer) aggregationBuffer).res = "";
        }

        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            Object o = objects[0];
            if (o != null) {
                StrAggregationBuffer mapAggregationBuffer = (StrAggregationBuffer) aggregationBuffer;
                String key = PrimitiveObjectInspectorUtils.getString(o, inputOI);
                mapAggregationBuffer.res = key;
            }
        }

        public void merge(AggregationBuffer aggregationBuffer, Object partial) throws HiveException {
            if (null != partial) {
                StrAggregationBuffer mapAggregationBuffer = (StrAggregationBuffer) aggregationBuffer;
                String partialStr = inputOI.getPrimitiveJavaObject(partial).toString();
                mapAggregationBuffer.res = partialStr;
            }
        }

        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            return terminate(aggregationBuffer);
        }

        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            StrAggregationBuffer mapAggregationBuffer = (StrAggregationBuffer) aggregationBuffer;
            return new Text(mapAggregationBuffer.res);
        }
    }
}
