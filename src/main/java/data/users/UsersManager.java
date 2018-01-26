package data.users;

import data.OperationException;

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

  public List<User> getAllUsers() throws OperationException {
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
      throw new OperationException(e.getMessage());
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

  public void addUser(User user) throws OperationException {
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
      throw new OperationException(e.getMessage());
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

  public HotelReservation getHotelReservation(int userId, int hotelId) throws OperationException {
    PreparedStatement statement = null;
    ResultSet rs = null;
    HotelReservation hotelReservation = null;
    try {
      String query = "SELECT * " +
          "FROM hotel_reservations WHERE user_id = ? and hotel_id = ?";
      statement = connection.prepareStatement(query);
      statement.setInt(1, userId);
      statement.setInt(2, hotelId);
      rs = statement.executeQuery();
      while (rs.next()) {
        Timestamp arrival = rs.getTimestamp(3);
        Timestamp departure = rs.getTimestamp(4);
        int noRooms = rs.getInt(5);

        hotelReservation = new HotelReservation(userId, hotelId, arrival, departure, noRooms);
      }
    } catch (SQLException e) {
      System.err.println(e.getMessage());
      throw new OperationException(e.getMessage());
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
    return hotelReservation;
  }

  public void reserveHotel(HotelReservation hotelReservation) throws OperationException {
    PreparedStatement statement = null;
    try {
      String query = "INSERT INTO hotel_reservations (user_id, hotel_id, arrival, departure, no_rooms) VALUES (?, ?, ?, ?, ?)";
      statement = connection.prepareStatement(query);
      statement.setInt(1, hotelReservation.getUserId());
      statement.setInt(2, hotelReservation.getHotelId());
      statement.setTimestamp(3, hotelReservation.getArrival());
      statement.setTimestamp(4, hotelReservation.getDeparture());
      statement.setInt(5, hotelReservation.getNoRooms());
      statement.executeUpdate();
    } catch (SQLException e) {
      System.err.println(e.getMessage());
      throw new OperationException(e.getMessage());
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

  public void removeHotelReservation(int userId, int hotelId) throws OperationException {
    PreparedStatement statement = null;
    try {
      String query = "DELETE FROM hotel_reservations WHERE user_id = ? AND hotel_id = ?";
      statement = connection.prepareStatement(query);
      statement.setInt(1, userId);
      statement.setInt(2, hotelId);
      statement.executeUpdate();
    } catch (SQLException e) {
      System.err.println(e.getMessage());
      throw new OperationException(e.getMessage());
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
