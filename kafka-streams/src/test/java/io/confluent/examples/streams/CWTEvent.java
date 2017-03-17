package io.confluent.examples.streams;

import java.io.*;
import java.util.UUID;

public class CWTEvent implements Serializable {
    private static final long serialVersionUID = 7526472295622776147L;

    public CWTEvent(String nhsNumber, Integer age, String address, String patientPathway, String referralStart,
            String treatmentStart) {
        NhsNumber = nhsNumber != null ? nhsNumber : "";
        Address = address = address != null ? address : "";
        Age = age;
        PatientIdentifierPathway = patientPathway != null ? patientPathway : "";
        CancerReferralTreatmentPeriodStart = referralStart != null ? referralStart : "";
        CancerTreatmentStartDate = treatmentStart != null ? treatmentStart : "";

        UniqueId = UUID.randomUUID().toString();
    }

    public CWTEvent(CWTEvent event) {
        NhsNumber = event.NhsNumber;
        Address = event.Address;
        Age = event.Age;
        PatientIdentifierPathway = event.PatientIdentifierPathway;
        CancerReferralTreatmentPeriodStart = event.CancerReferralTreatmentPeriodStart;
        CancerTreatmentStartDate = event.CancerTreatmentStartDate;
        
        UniqueId = UUID.randomUUID().toString();
    }

    public String UniqueId;
    public String NhsNumber;
    public Integer Age;
    public String Address;
    public String PatientIdentifierPathway;
    public String CancerReferralTreatmentPeriodStart;
    public String CancerTreatmentStartDate;

    public void update(CWTEvent event) {
        Age = nullCoalesce(event.Age, Age);
        Address = stringCoalesce(event.Address, Address);
        PatientIdentifierPathway = stringCoalesce(event.PatientIdentifierPathway, PatientIdentifierPathway);
        CancerReferralTreatmentPeriodStart = stringCoalesce(event.CancerReferralTreatmentPeriodStart,
                CancerReferralTreatmentPeriodStart);
        CancerTreatmentStartDate = stringCoalesce(event.CancerTreatmentStartDate, CancerTreatmentStartDate);
    }

    public <T extends Object> T nullCoalesce(T first, T second) {
        return first != null ? first : second;
    }

    public String stringCoalesce(String first, String second) {
        return first.length() > 0 ? first : second;
    }

    @Override
    public String toString() {
        return NhsNumber + "," + Age + "," + Address + "," + PatientIdentifierPathway;
    }

    public BreachData calculateBreachData(){
        return new BreachData();
    }
    
    public boolean hasPatientIdentifierPathway() {
        return PatientIdentifierPathway.length() > 0;
    }

    public boolean hasCancerReferralTreatmentPeriodStart() {
        return CancerReferralTreatmentPeriodStart.length() > 0;
    }

    public boolean hasCancerTreatmentStartDate() {
        return CancerTreatmentStartDate.length() > 0;
    }

    public boolean matches(CWTEvent other) {
        return NhsNumber.equals(other.NhsNumber) && (PatientIdentifierPathway.equals(other.PatientIdentifierPathway)
                || CancerReferralTreatmentPeriodStart.equals(other.CancerReferralTreatmentPeriodStart)
                || CancerTreatmentStartDate.equals(other.CancerTreatmentStartDate));
    }

    @Override
    public boolean equals(Object o) {
        try {
            if (o == this) {
                return true;
            }

            if (!(o instanceof CWTEvent)) {
                return false;
            }

            CWTEvent other = (CWTEvent) o;

            return ((Age != null && Age.equals(other.Age)) || other.Age == null) && Address.equals(other.Address)
                    && PatientIdentifierPathway.equals(other.PatientIdentifierPathway)
                    && CancerReferralTreatmentPeriodStart.equals(other.CancerReferralTreatmentPeriodStart)
                    && CancerTreatmentStartDate.equals(other.CancerTreatmentStartDate)
                    && NhsNumber.equals(other.NhsNumber);

        } catch (NullPointerException e) {
            System.out.println("other " + o);
            System.out.println("Age " + Age);
            System.out.println("AddressAge " + Address);

            System.out.println("PatientIdentifierPathway " + PatientIdentifierPathway);
            System.out.println("CancerReferralTreatmentPeriodStart " + CancerReferralTreatmentPeriodStart);
            System.out.println("NhsNumber " + NhsNumber);

            return false;
        }
    }
}