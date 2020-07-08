package com.sudoprivacy.udaf.StrCntMap.Abandoned;


import com.sudoprivacy.enums.UdfDataType;
import com.sudoprivacy.enums.UdfOuputType;
import com.sudoprivacy.enums.UdfProcesType;
import com.sudoprivacy.udaf.StrCntMap.BaseStrCntMapEvaluator;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

@Description(
        name = "sum_strlist",
        value = "Return total sum of all str in lists as str format.",
        extended = "Example:\n > SELECT sum_str_list(col) from table;"
)
public class SumStrList extends AbstractGenericUDAFResolver {
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
    public static class MaxStrEvaluator extends BaseStrCntMapEvaluator {

        @Override
        protected UdfDataType InputType() throws HiveException {
            return UdfDataType.ListAsStr;
        }

        @Override
        protected UdfOuputType OutputType() throws HiveException {
            return UdfOuputType.MapSum;
        }

        @Override
        protected UdfProcesType ProcessType() throws HiveException {
            return UdfProcesType.List;
        }
    }
}
