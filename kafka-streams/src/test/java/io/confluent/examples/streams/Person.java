package io.confluent.examples.streams;

import java.io.*;


public class Person implements Serializable {
    private static final long serialVersionUID = 7526472295622776147L;

    public Person(String nhsNumber, Integer age, String address, String patientPathway, String referralStart, String treatmentStart) {
        NhsNumber = nhsNumber != null ? nhsNumber : "";
        Address = address = address != null ? address : "";
        Age = age;
        PatientIdentifierPathway = patientPathway != null ? patientPathway : "";
        CancerReferralTreatmentPeriodStart = referralStart != null ? referralStart : "";
        CancerTreatmentStartDate = treatmentStart != null ? treatmentStart : "";
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

        return ((Age == null && other.Age == null) || Age.equals(other.Age)) 
                && Address.equals(other.Address)
                && PatientIdentifierPathway.equals(other.PatientIdentifierPathway)
                && CancerReferralTreatmentPeriodStart.equals(other.CancerReferralTreatmentPeriodStart)
                && CancerTreatmentStartDate.equals(other.CancerTreatmentStartDate)
                && NhsNumber.equals(other.NhsNumber);
    }
}