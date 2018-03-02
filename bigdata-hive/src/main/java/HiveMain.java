import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by JackManWu on 2018/2/26.
 */
public class HiveMain {
    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        String url = "jdbc:hive2://master:10000/test";
        Connection connection = DriverManager.getConnection(url,"wujinlei","wujinlei");
        String sql = "select * from default.tb_order";
        PreparedStatement statement = connection.prepareStatement(sql);
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            System.out.println(resultSet.getInt(1) + "    " + resultSet.getString(2) + "  " + resultSet.getDouble(3));
        }
    }
}
