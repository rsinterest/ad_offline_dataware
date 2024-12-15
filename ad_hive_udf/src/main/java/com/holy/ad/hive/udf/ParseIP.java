package com.holy.ad.hive.udf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IOUtils;
import org.lionsoul.ip2region.xdb.Searcher;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;

public class ParseIP extends GenericUDF {
    Searcher searcher;
    /**
     * 判断函数传入的参数个数以及类型，同时确定返回的类型
     * @param objectInspectors
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        // 传入两个参数，一个是本地IP数据库的地址信息（这个数据库在hdfs的路径），第二个是ipv4地址
        // 判断参数个数
        if(objectInspectors.length != 2){
            throw new UDFArgumentException("parseIP必须填写2个参数");
        }
        // 校验参数的类型，两个参数都必须是字符串
        ObjectInspector hdfspathOI = objectInspectors[0];
        if (hdfspathOI.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("parseIP的第一个参数必须是基本数据类型");
        }

        PrimitiveObjectInspector hdfspathOI1 = (PrimitiveObjectInspector) hdfspathOI;
        if (hdfspathOI1.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("parseIP的第一个参数必须是字符串类型");
        }

        ObjectInspector ipOI = objectInspectors[1];
        if (ipOI.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("parseIP的第二个参数必须是基本数据类型");
        }

        PrimitiveObjectInspector ipOI1 = (PrimitiveObjectInspector) ipOI;
        if (ipOI1.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("parseIP的第二个参数必须是字符串类型");
        }
        // 读取ip静态库进入内存当中
        // 获取hdfs的路径
        if (hdfspathOI instanceof ConstantObjectInspector) {
            String hdfsPath = ((ConstantObjectInspector) hdfspathOI).getWritableConstantValue().toString();
            //从hdfs中读取路径
            Path path = new Path(hdfsPath);
            try {
                FileSystem fileSystem = FileSystem.get(new Configuration());
                //读取path路径
                FSDataInputStream inputStream = fileSystem.open(path);  //这里还没读出来
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                IOUtils.copyBytes(inputStream, byteArrayOutputStream, 1024);   // 这样读完才会有数据
                byte[] byteArray = byteArrayOutputStream.toByteArray();

                // 创建静态库解析ip对象
                searcher = Searcher.newWithBuffer(byteArray);

            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        //确定函数返回的数据结构
        ArrayList<String> structFieldNames = new ArrayList<>();
        structFieldNames.add("country");
        structFieldNames.add("area");
        structFieldNames.add("province");
        structFieldNames.add("city");
        structFieldNames.add("isp");
        ArrayList<ObjectInspector> structFieldObjectInspectors = new ArrayList<>();
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);
    }

    /**
     * 处理数据
     * @param deferredObjects
     * @return
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        String ip = deferredObjects[1].get().toString();
        ArrayList<Object> result = new ArrayList<>();

        try {
            String search = searcher.search(ip);
            String[] split = search.split("\\|");
            result.add(split[0]);
            result.add(split[1]);
            result.add(split[2]);
            result.add(split[3]);
            result.add(split[4]);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

    /**
     * 写一个描述函数
     * @param strings
     * @return
     */
    @Override
    public String getDisplayString(String[] strings) {
        return this.getStandardDisplayString("parseIP", strings);
    }
}
