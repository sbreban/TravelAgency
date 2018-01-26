package data.hotels;

public class Hotel {
  private int id;
  private String name;
  private String city;
  private String address;
  private int stars;
  private int capacity;

  public Hotel(int id, String name, String city, String address, int stars, int capacity) {
    this.id = id;
    this.name = name;
    this.city = city;
    this.address = address;
    this.stars = stars;
    this.capacity = capacity;
  }

  @Override
  public String toString() {
    return "Hotel{" +
        "id=" + id +
        ", name='" + name + '\'' +
        ", city='" + city + '\'' +
        ", address='" + address + '\'' +
        ", stars=" + stars +
        ", capacity=" + capacity +
        '}';
  }
}
