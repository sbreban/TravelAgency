package users;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UsersManager {

  private Connection connection;

  public UsersManager() {
    // create a database connection
    try {
      connection = DriverManager.getConnection("jdbc:sqlite:users.db");
    } catch (SQLException e) {
      System.err.println(e);
    }
  }

  public List<User> getAllUsers() {
    PreparedStatement statement = null;
    ResultSet rs = null;
    List<User> users = new ArrayList<>();
    try {
      String query = "SELECT * " +
          "FROM users";
      statement = connection.prepareStatement(query);
      rs = statement.executeQuery();
      while (rs.next()) {
        // read the result set
        User user = extractUser(rs);

        users.add(user);
      }
    } catch (SQLException e) {
      System.err.println(e.getMessage());
    } finally {
      try {
        if (rs != null) {
          rs.close();
        }
        if (statement != null) {
          statement.close();
        }
      } catch (SQLException e) {
        System.err.println(e);
      }
    }
    return users;
  }

  private User extractUser(ResultSet rs) throws SQLException {
    int userId = rs.getInt(1);
    String name = rs.getString(2);
    int age = rs.getInt(3);
    return new User(userId, name, age);
  }

  public void addUser(User user) {
    PreparedStatement statement = null;
    try {
      String query = "INSERT INTO users (id, name, age) VALUES (?, ?, ?)";
      statement = connection.prepareStatement(query);
      statement.setInt(1, user.getId());
      statement.setString(2, user.getName());
      statement.setInt(3, user.getAge());
      statement.executeUpdate();
    } catch (SQLException e) {
      System.err.println(e.getMessage());
    } finally {
      try {
        if (statement != null) {
          statement.close();
        }
      } catch (SQLException e) {
        System.err.println(e);
      }
    }
  }

  public void close() {
    try {
      if (connection != null)
        connection.close();
    } catch (SQLException e) {
      // connection close failed.
      System.err.println(e);
    }
  }
}
