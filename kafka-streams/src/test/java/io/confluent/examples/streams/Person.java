package io.confluent.examples.streams;


  public class Person{
      public Person(String nhsNumber, Integer age, String address){
        this.Address = address;
        this.Age = age;
        this.NhsNumber = nhsNumber;
      }

      public Person(){
        Address = "this shouldn't happen";
      }

      public String NhsNumber;
      public Integer Age;
      public String Address;

      @Override
      public String toString(){
        return "USER: " + NhsNumber + "," + Age + "," + Address;
      }
  }
