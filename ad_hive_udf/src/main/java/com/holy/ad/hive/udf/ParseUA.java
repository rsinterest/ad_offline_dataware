package com.holy.ad.hive.udf;

import cn.hutool.http.useragent.UserAgent;
import cn.hutool.http.useragent.UserAgentUtil;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.eclipse.jetty.server.Authentication;
import org.lionsoul.ip2region.xdb.Searcher;

import java.util.ArrayList;

public class ParseUA extends GenericUDF {
    Searcher searcher;
    /**
     * 判断函数传入的参数个数以及类型，同时确定返回的类型
     * @param objectInspectors
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        // 传入1个参数就是解析过后的ua
        // 判断参数个数
        if(objectInspectors.length != 1){
            throw new UDFArgumentException("parseUA必须填写1个参数");
        }
        // 校验参数的类型，参数类型为字符串
        ObjectInspector uaOI = objectInspectors[0];
        if (uaOI.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("parseUA的第一个参数必须是基本数据类型");
        }

        PrimitiveObjectInspector uaOI1 = (PrimitiveObjectInspector) uaOI;
        if (uaOI1.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("parseUA的第一个参数必须是字符串类型");
        }

        //确定函数返回的数据结构
        ArrayList<String> structFieldNames = new ArrayList<>();
        structFieldNames.add("browser");
        structFieldNames.add("browserVersion");
        structFieldNames.add("engine");
        structFieldNames.add("engineVersion");
        structFieldNames.add("os");
        structFieldNames.add("osVersion");
        structFieldNames.add("platform");
        structFieldNames.add("isMobile");
        ArrayList<ObjectInspector> structFieldObjectInspectors = new ArrayList<>();
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        String ua = deferredObjects[0].get().toString();
        UserAgent parse = UserAgentUtil.parse(ua);
        ArrayList<Object> result = new ArrayList<>();
        result.add(parse.getBrowser().getName());
        result.add(parse.getVersion());
        result.add(parse.getEngine().getName());
        result.add(parse.getEngineVersion());
        result.add(parse.getOs().getName());
        result.add(parse.getOsVersion());
        result.add(parse.getPlatform().getName());
        result.add(parse.isMobile());

        return result;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return this.getStandardDisplayString("parseUA", strings);
    }
}
