package data.users;

import java.sql.Timestamp;

public class HotelReservation {
  private final int userId;
  private final int hotelId;
  private final Timestamp arrival;
  private final Timestamp departure;
  private final int noRooms;

  public HotelReservation(int userId, int hotelId, Timestamp arrival, Timestamp departure, int noRooms) {
    this.userId = userId;
    this.hotelId = hotelId;
    this.arrival = arrival;
    this.departure = departure;
    this.noRooms = noRooms;
  }

  public int getUserId() {
    return userId;
  }

  public int getHotelId() {
    return hotelId;
  }

  public Timestamp getArrival() {
    return arrival;
  }

  public Timestamp getDeparture() {
    return departure;
  }

  public int getNoRooms() {
    return noRooms;
  }
}
