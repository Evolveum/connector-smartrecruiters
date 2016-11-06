package com.evolveum.polygon.connector.smartrecruiters;

/**
 * @author gpalos
 */
public class SmartRecruitersFilter {
    public String byUid;
    public String byEmailAddress;

    @Override
    public String toString() {
        return "SmartRecruitersFilter{" +
                "byUid=" + byUid +
                ", byEmailAddress='" + byEmailAddress + '\'' +
                '}';
    }
}
