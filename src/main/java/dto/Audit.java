package dto;

import java.util.Date;

public class Audit implements java.io.Serializable{

   private String auditLog;
   private String dbLastUpdatedDate;

   private String currentStatus;
   private String priviousStatus;
   private Double daysCount;


   public String getAuditLog() {
      return auditLog;
   }

   public void setAuditLog(String auditLog) {
      this.auditLog = auditLog;
   }

   public String getDbLastUpdatedDate() {
      return dbLastUpdatedDate;
   }

   public void setDbLastUpdatedDate(String dbLastUpdatedDate) {
      this.dbLastUpdatedDate = dbLastUpdatedDate;
   }



    public String getCurrentStatus() {
        return currentStatus;
    }

    public void setCurrentStatus(String currentStatus) {
        this.currentStatus = currentStatus;
    }

    public String getPriviousStatus() {
        return priviousStatus;
    }

    public void setPriviousStatus(String priviousStatus) {
        this.priviousStatus = priviousStatus;
    }

    public Double getDaysCount() {
        return daysCount;
    }

    public void setDaysCount(Double daysCount) {
        this.daysCount = daysCount;
    }
}
