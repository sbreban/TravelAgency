package hotels;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class HotelsManager {

  private Connection connection;

  public HotelsManager() {
    // create a database connection
    try {
      connection = DriverManager.getConnection("jdbc:sqlite:hotels.db");
    } catch (SQLException e) {
      System.err.println(e);
    }
  }

  public List<Hotel> getAllHotels() {
    PreparedStatement statement = null;
    ResultSet rs = null;
    List<Hotel> hotels = new ArrayList<>();
    try {
      String query = "SELECT * " +
          "FROM hotels";
      statement = connection.prepareStatement(query);
      rs = statement.executeQuery();
      while (rs.next()) {
        // read the result set
        Hotel hotel = extractHotel(rs);
        hotels.add(hotel);
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
    return hotels;
  }

  public List<Hotel> getHotels(String toDestination) {
    PreparedStatement statement = null;
    ResultSet rs = null;
    List<Hotel> hotels = new ArrayList<>();
    try {
      String query = "SELECT * " +
          "FROM hotels h " +
          "WHERE h.city = ?";
      statement = connection.prepareStatement(query);
      statement.setString(1, toDestination);
      rs = statement.executeQuery();
      while (rs.next()) {
        Hotel hotel = extractHotel(rs);

        hotels.add(hotel);
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
    return hotels;
  }

  private Hotel extractHotel(ResultSet rs) throws SQLException {
    // read the result set
    int hotelId = rs.getInt(1);
    String name = rs.getString(2);
    String city = rs.getString(3);
    String address = rs.getString(4);
    int stars = rs.getInt(5);
    int capacity = rs.getInt(6);
    return new Hotel(hotelId, name, city, address, stars, capacity);
  }
}
