package io.confluent.examples.streams;


  public class User{
      public User(String nhsNumber, Integer age, String address){
        this.Address = address;
        this.Age = age;
        this.NhsNumber = nhsNumber;
      }
      public String NhsNumber;
      public Integer Age;
      public String Address;
  }
