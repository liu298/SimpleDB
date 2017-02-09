import java.sql.*;
import simpledb.remote.SimpleDriver;

public class TestGroupBy {
    public static void main(String[] args) {
    	// String name = args[0];
    	int year = 2005;
		Connection conn = null;
		try {
			// Step 1: connect to database server
			
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);

			// Step 2: execute the query
			Statement stmt = conn.createStatement();
			String qry = "select count(SId), GradYear "
			           + "from STUDENT "
			           + "group by GradYear";
			ResultSet rs = stmt.executeQuery(qry);

			// Step 3: loop through the result set
			System.out.println("countofSId\tGradYear");
			while (rs.next()) {
				int sid = rs.getInt("countofsid");
				int gradyear = rs.getInt("GradYear");
				System.out.println(sid +"\t" + gradyear);
			}
			rs.close();
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
		finally {
			// Step 4: close the connection
			try {
				if (conn != null)
					conn.close();
			}
			catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}