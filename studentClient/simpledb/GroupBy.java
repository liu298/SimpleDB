import java.sql.*;
import simpledb.remote.SimpleDriver;

public class GroupBy {
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
			String qry = "select dname, max(GradYear) "
			           + "from DEPT, STUDENT "
			           + "where MajorId = DId " 
			           + "group by dname";
			           // + "and GradYear = 2005";
			ResultSet rs = stmt.executeQuery(qry);

			// Step 3: loop through the result set
			System.out.println("Major\tmaxofGradYear");
			while (rs.next()) {
				// int sid = rs.getInt("SId");
				// String sname = rs.getString("SName");
				String dname = rs.getString("DName");
				int max = rs.getInt("maxofGradYear");
				System.out.println( dname + " \t " + max);
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