import java.sql.*;
import simpledb.remote.SimpleDriver;

public class FindMajors {
    public static void main(String[] args) {
		String major = args[0];
		System.out.println("Here are the " + major + " majors");
		System.out.println("Name\tGradYear");

		Connection conn = null;
		try {
			// Step 1: connect to database server
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);

			// Step 2: execute the query
			Statement stmt = conn.createStatement();
			String qry = "select did, dname "
			           + "from dept "
			           // + "where did = majorid "
			           + "where dname = '" + major + "'";
			ResultSet rs = stmt.executeQuery(qry);

			// Step 3: loop through the result set
			while (rs.next()) {
				int did = rs.getInt("did");
				String dname = rs.getString("dname");
				System.out.println(did + "\t" + dname);
			}
			rs.close();
		}
		catch(Exception e) {
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
