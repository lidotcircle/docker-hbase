import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * compile: javac -cp $(hbase classpath) HBaseAPI.java
 * run:     java  -cp $(hbase classpath) HBaseAPI
 */
public class HBaseAPI {

    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(config);
        Admin admin = conn.getAdmin();

        // create_namespace "school"
        admin.createNamespace(NamespaceDescriptor.create("school").build());

        // create "school:user", {NAME => "essential", VERSIONS => 3, MIN_VERSIONS => 2}, {NAME => "additional", VERSIONS => 3, MIN_VERSIONS => 2}
        HTableDescriptor tuser = new HTableDescriptor(TableName.valueOf("school:user"));;
        HColumnDescriptor colEssential  = new HColumnDescriptor(Bytes.toBytes("essential"));
        HColumnDescriptor colAdditional = new HColumnDescriptor(Bytes.toBytes("additional"));
        colEssential.setVersions(2, 3);
        colAdditional.setVersions(2, 3);
        tuser.addFamily(colEssential);
        tuser.addFamily(colAdditional);
        admin.createTable(tuser);

        // create "teacher", "essential"
        HTableDescriptor tteacher = new HTableDescriptor(TableName.valueOf("teacher"));;
        HColumnDescriptor tcolEssential  = new HColumnDescriptor(Bytes.toBytes("essential"));
        tteacher.addFamily(tcolEssential);
        admin.createTable(tteacher);

        // alter "teacher", {NAME => "additional"}, {NAME => "essential"}
        HColumnDescriptor tcolAdditional = new HColumnDescriptor(Bytes.toBytes("additional"));
        admin.addColumn(TableName.valueOf("teacher"), tcolAdditional);
        admin.deleteColumn(TableName.valueOf("teacher"), Bytes.toBytes("essential"));

        // disable "teacher"
        admin.disableTable(TableName.valueOf("teacher"));

        // drop "teacher"
        admin.deleteTable(TableName.valueOf("teacher"));

        Table user = conn.getTable(TableName.valueOf("school:user"));
        // put "school:user", "rk1", "essential:name", "ldy"
        Put rk1 = new Put(Bytes.toBytes("rk1"));
        rk1.addColumn(Bytes.toBytes("essential"), Bytes.toBytes("name"), Bytes.toBytes("ldy"));
        user.put(rk1);

        // put "school:user", "rk1", "essential:sex", "male"
        rk1 = new Put(Bytes.toBytes("rk1"));
        rk1.addColumn(Bytes.toBytes("essential"), Bytes.toBytes("sex"), Bytes.toBytes("male"));
        user.put(rk1);

        // put "school:user", "rk1", "additional:interst", "coding"
        rk1 = new Put(Bytes.toBytes("rk1"));
        rk1.addColumn(Bytes.toBytes("additional"), Bytes.toBytes("interst"), Bytes.toBytes("coding"));
        user.put(rk1);

        // put "school:user", "rk1", "additional:interst", "coding and running"
        rk1 = new Put(Bytes.toBytes("rk1"));
        rk1.addColumn(Bytes.toBytes("additional"), Bytes.toBytes("interst"), Bytes.toBytes("coding and running"));
        user.put(rk1);

        // put "school:user", "rk1", "additional:interst", "coding, running and sleeping"
        rk1 = new Put(Bytes.toBytes("rk1"));
        rk1.addColumn(Bytes.toBytes("additional"), Bytes.toBytes("interst"), Bytes.toBytes("coding, running and sleeping"));
        user.put(rk1);

        // scan "schoo:user"
        ResultScanner scannerResult = user.getScanner(new Scan());
        for(Result result=scannerResult.next(); result!=null; result=scannerResult.next()) {
            System.out.println(result);
        }

        conn.close();
    }
}

