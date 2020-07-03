package com.sudoprivacy.udaf;


import com.sudoprivacy.enums.UdfDataType;
import com.sudoprivacy.enums.UdfOuputType;
import com.sudoprivacy.enums.UdfProcesType;
import com.sudoprivacy.udaf.common.MapStrCountEvaluator;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

@Description(
        name = "max_cnt_strset",
        value = "Return  count of the most frequent str in sets.",
        extended = "Example:\n > SELECT max_cnt_set(col) from table;"
)
public class MaxCountStrSet extends AbstractGenericUDAFResolver {
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

        @Override
        protected UdfDataType InputType() throws HiveException {
            return UdfDataType.ListAsStr;
        }

        @Override
        protected UdfOuputType OutputType() throws HiveException {
            return UdfOuputType.MapMaxCount;
        }

        @Override
        protected UdfProcesType ProcessType() throws HiveException {
            return UdfProcesType.Set;
        }
    }
}
