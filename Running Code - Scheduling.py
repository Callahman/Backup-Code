from time import sleep
import schedule



### Reports
import SC_LOB_Error_Report
import Homebase_Lat_Long_Check
import Duplicate_Location_Warning
import Daily_Automation_Status_Report
import Base_Calendar_Alert
import Absence_Deletion_Tracking
import Absence_Cleanup_Report


### Hourly
schedule.every().hour.do(Base_Calendar_Alert.Report)

### Daily
schedule.every().day.at("6:00").do(Daily_Automation_Status_Report.Daily_Message)
schedule.every().day.at("20:00").do(Daily_Automation_Status_Report.Daily_Message)
schedule.every().day.at("6:00").do(Absence_Deletion_Tracking.Report)
schedule.every().day.at("6:00").do(Duplicate_Location_Warning.Report)  ### 10/5/18

### Weekly
schedule.every().monday.at("6:00").do(SC_LOB_Error_Report.Report)
schedule.every().monday.at("6:00").do(Homebase_Lat_Long_Check.Report)
schedule.every().monday.at("6:00").do(Absence_Cleanup_Report.Report)



while True:
    schedule.run_pending()
    sleep(1)