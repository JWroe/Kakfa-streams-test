package io.confluent.examples.streams;

public class Person {
    public Person(String nhsNumber, Integer age, String address) {
        this.NhsNumber = nhsNumber != null ? nhsNumber : "";
        this.Age = age;
        this.Address = address = address != null ? address : "";
    }

    public String NhsNumber;
    public Integer Age;
    public String Address;
    public String PatientIdentifierPathway;
    public String CancerReferralTreatmentPeriodStart;
    public String CancerTreatmentStartDate;

    @Override
    public String toString() {
        return "USER: " + NhsNumber + "," + Age + "," + Address;
    }

    public boolean matches(Person other){
        return NhsNumber == other.NhsNumber && 
                (PatientIdentifierPathway == other.PatientIdentifierPathway ||
                CancerReferralTreatmentPeriodStart == other.CancerReferralTreatmentPeriodStart ||
                CancerTreatmentStartDate == other.CancerTreatmentStartDate);
    }

    @Override
    public boolean equals(Object o) { 
        if (o == this) {
            return true;
        }

        if (!(o instanceof Person)) {
            return false;
        }

        Person other = (Person) o;

        return ((Age == null && other.Age == null) || Age.equals(other.Age)) && Address.equals(other.Address)
                && NhsNumber.equals(other.NhsNumber);
    }
}