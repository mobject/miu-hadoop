import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

public class MyFirstHbaseTable {

    private static final String TABLE_NAME = "user";
    private static final String CF_PERSONAL_DETAILS = "personal_details";
    private static final String CF_PROF_DETAILS = "prof_details";
    private static final String CQ_NAME = "Name";
    private static final String CQ_CITY = "City";
    private static final String CQ_DESIGNATION = "Designation";
    private static final String CQ_SALARY = "Salary";

    public static void main(String... args) throws IOException {

        Configuration config = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            table.addFamily(new HColumnDescriptor(CF_PERSONAL_DETAILS).setCompressionType(Algorithm.NONE));
            table.addFamily(new HColumnDescriptor(CF_PROF_DETAILS));

            System.out.print("Creating table.... ");

            if (admin.tableExists(table.getTableName())) {
                admin.disableTable(table.getTableName());
                admin.deleteTable(table.getTableName());
            }
            admin.createTable(table);

            //question 1
            Table userTable = connection.getTable(TableName.valueOf(TABLE_NAME));
            byte[] empId1 = Bytes.toBytes(1);
            Put putRow1 = new Put(empId1);
            putRow1.addColumn(CF_PERSONAL_DETAILS.getBytes(), CQ_NAME.getBytes(), "John".getBytes());
            putRow1.addColumn(CF_PERSONAL_DETAILS.getBytes(), CQ_CITY.getBytes(), "Boston".getBytes());
            putRow1.addColumn(CF_PROF_DETAILS.getBytes(), CQ_DESIGNATION.getBytes(), "Manager".getBytes());
            putRow1.addColumn(CF_PROF_DETAILS.getBytes(), CQ_SALARY.getBytes(), Bytes.toBytes(150000));
            userTable.put(putRow1);

            byte[] empId2 = Bytes.toBytes(2);
            Put putRow2 = new Put(empId2);
            putRow2.addColumn(CF_PERSONAL_DETAILS.getBytes(), CQ_NAME.getBytes(), "Mary".getBytes());
            putRow2.addColumn(CF_PERSONAL_DETAILS.getBytes(), CQ_CITY.getBytes(), "New York".getBytes());
            putRow2.addColumn(CF_PROF_DETAILS.getBytes(), CQ_DESIGNATION.getBytes(), "Sr. Engineer".getBytes());
            putRow2.addColumn(CF_PROF_DETAILS.getBytes(), CQ_SALARY.getBytes(), Bytes.toBytes(130000));
            userTable.put(putRow2);

            byte[] empId3 = Bytes.toBytes(3);
            Put putRow3 = new Put(empId3);
            putRow3.addColumn(CF_PERSONAL_DETAILS.getBytes(), CQ_NAME.getBytes(), "Bob".getBytes());
            putRow3.addColumn(CF_PERSONAL_DETAILS.getBytes(), CQ_CITY.getBytes(), "Fremont".getBytes());
            putRow3.addColumn(CF_PROF_DETAILS.getBytes(), CQ_DESIGNATION.getBytes(), "Jr. Engineer".getBytes());
            putRow3.addColumn(CF_PROF_DETAILS.getBytes(), CQ_SALARY.getBytes(), Bytes.toBytes(90000));
            userTable.put(putRow3);

            //question 2
            Get getBob = new Get(empId3);
            Result result = userTable.get(getBob);
            byte[] salaryByte = result.getValue(CF_PROF_DETAILS.getBytes(), CQ_SALARY.getBytes());
            double salary = (double) Bytes.toInt(salaryByte);
            double promotedSalary = (salary * 5 / 100) + salary;
            Put updateBob = new Put(empId3);
            updateBob.addColumn(CF_PROF_DETAILS.getBytes(), CQ_DESIGNATION.getBytes(), "Sr. Engineer".getBytes());
            updateBob.addColumn(CF_PROF_DETAILS.getBytes(), CQ_SALARY.getBytes(), Bytes.toBytes(promotedSalary));
            userTable.put(updateBob);

            //question 3

            System.out.println(" Done!");
        }
    }
}