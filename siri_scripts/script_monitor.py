# -*- coding: utf-8 -*-
import time
from datetime import datetime, time as dt_time, timedelta
from common.utils import (RedisConnection, logger_setup, delete_old_log_files)
import requests
import logging
import sys
from azure.communication.email import EmailClient
class MonitoringType:
    FIXED = "FIXED"
    CONTINUOUS = "CONTINUOUS"
    SERVER = "SERVER"




MAX_LOGS_DAYS = 7
console_logging_level = logging.DEBUG
file_logging_level = logging.DEBUG


RECIPIENTS = [{"address": "ayaz4049@yahoo.com", "displayName": "Ayaz"},
              {"address": "alons@gl-urban.co.il", "displayName": "Alon"},
              {"address": "sharon@gl-urban.co.il", "displayName": "Sharon"},
              {"address": "noamziv@gl-urban.co.il", "displayName": "Noam"}]

# monitoring frequecy in minutes.
MONITORING_FREQUENCY = 5
# time for sending daily update email.
DAILY_NOTIFICATION_TIME = "07:15"
# flag for sending daily email.
SEND_DAILY_EMAIL = True

# specify max_down_time in minutes.
configuration = {"GTFS-LOADER": {"key": "gtfs_loader", "name": "GTFS-LOADER",
                               "active_time": "06:30", "monitor_type": MonitoringType.FIXED, "max_down_freq": 1, "max_down_time": 60},
                "SIRI-LOADER": {"key": "siri_loader", "name": "SIRI-LOADER Script",
                                      "active_time": "05:00-23:00", "monitor_type": MonitoringType.CONTINUOUS, "max_down_freq": 1, "max_down_time": 10},
                "TRIP-PERFORMANCE": {"key": "trip_performance", "name": "TRIP-PERFORMANCE Script",
                                      "active_time": "07:00-23:00", "monitor_type": MonitoringType.CONTINUOUS, "max_down_freq": 1, "max_down_time": 10},
                "TRIP-DEVIATION": {"key": "trip_deviation", "name": "DEVIATION Script",
                                      "active_time": "07:00-23:00", "monitor_type": MonitoringType.CONTINUOUS, "max_down_freq": 1, "max_down_time": 10},
                "TRIP-LOW-SPEED": {"key": "trip_low_speed", "name": "LOW-SPEED Script",
                                      "active_time": "07:00-23:00", "monitor_type": MonitoringType.CONTINUOUS, "max_down_freq": 1, "max_down_time": 10},
                "BUNCHING": {"key": "trip_bunching", "name": "BUNCHING Script",
                                      "active_time": "07:00-23:00", "monitor_type": MonitoringType.CONTINUOUS, "max_down_freq": 1, "max_down_time": 10},
                "Server": {"key": "FLASK_SERVER", "name": "Flask Server", "api_url":"http://64.176.167.2:5000/api/v1/misc/server-status",
                            "active_time": "07:00-23:00", "monitor_type": MonitoringType.SERVER, "max_down_freq": 3},
                "NGNIX": {"key": "NGNIX_SERVER", "name": "NGNIX Server", "api_url":"https://alerts.j.gl-urban.com/api/v1/misc/server-status",
                            "active_time": "07:00-23:00", "monitor_type": MonitoringType.SERVER, "max_down_freq": 3}
                 }


class MonitorStatus(RedisConnection):
    def __init__(self, logger, script_key, script_name, monitor_type, active_time, max_down_time=5,
                 max_down_freq=1, api_url=None, host='localhost', port=6379, timeout=20, expiry=None):
        super().__init__(logger=logger, host=host, port=port, timeout=timeout, expiry=expiry)
        self.logger = logger
        self.script_key = script_key
        self.script_name = script_name
        self.monitor_type = monitor_type
        self.max_down_time = max_down_time * 60  #converting to seconds.
        self.max_down_freq = max_down_freq if max_down_freq else 1
        self.down_freq = 0
        self.active_time = active_time
        self.start_time = None
        self.end_time = None
        self.stop_monitoring = False
        self.trigger_time = None
        self.trigger_message = None
        self.api_url = api_url
        self.set_active_time()
        self.logger.info(f"Starting {self.monitor_type} monitoring for {self.script_key}")

    def update_logger(self, logger):
        self.logger = None
        self.logger = logger

    def get_last_signal_time(self):
        try:
            last_signal_time = self.get_key(self.script_key)
            if last_signal_time:
                return int(last_signal_time)
        except Exception as error:
            self.logger.exception(f"Unable to get last_signal time for {self.script_key}, due to {error}")

    def start_monitoring(self):
        if (self.monitor_type==MonitoringType.CONTINUOUS):
            return self.start_monitoring_continuous()
        elif(self.monitor_type==MonitoringType.FIXED):
            return self.start_monitoring_fixed()
        elif (self.monitor_type == MonitoringType.SERVER):
            return self.start_monitoring_server()
        else:
            self.logger.exception(f"Unknown Monitoring Type, {self.monitor_type} for {self.script_name}")
            raise Exception(f"Unknown Monitoring Type, {self.monitor_type} for {self.script_name}")

    def start_monitoring_continuous(self):
        try:
            if self.is_active_time() and not self.stop_monitoring:
                last_signal_time = self.get_last_signal_time()
                if last_signal_time is not None:
                    time_diff = time.time() - last_signal_time
                    if time_diff > self.max_down_time:
                        self.down_freq += 1
                        self.logger.critical(f"{self.script_key}: signal_time is behind, down_freq={self.down_freq}.")
                        self.trigger_message = f"Signal_time is behind current time, time_diff={round(time_diff/60,2)} minutes."
                    else:
                        self.logger.debug(f"{self.script_key} script is running fine.")
                        return True
                else:
                    self.logger.critical(f"{self.script_key}: signal_time not found, down_freq={self.down_freq}.")
                    self.trigger_message = f"Signal_time not found. It should not be None."
                    self.down_freq += 1

                if self.down_freq >= self.max_down_freq:
                    # send notification and stop monitoring it now.
                    self.stop_monitoring = True
                    self.trigger_time = time.time()
                    self.logger.info(f"{self.script_key} script not running, triggering email. down_freq={self.down_freq}.")
                    self.sent_notification(last_signal_time=last_signal_time)

            elif self.is_active_time() and self.stop_monitoring:
                # define criteria to start monitoring again.
                last_signal_time = self.get_last_signal_time()
                # if script started again, then start monitoring again.
                if last_signal_time is not None and last_signal_time > self.trigger_time:
                    self.reset_monitor()
                else:
                    self.logger.debug(f"{self.script_key}: Already notified for {self.script_key} failure.")
            else:
                self.logger.debug(f"{self.script_key}: Off time. active_hours {self.start_time}-{self.end_time}.")

        except Exception as error:
            self.logger.exception(f"{self.script_key}: Something went wrong in monitoring due to {error}.")
            self.down_freq += 1
            if self.down_freq >= self.max_down_freq:
                self.stop_monitoring = True
                self.trigger_time = time.time()
                self.trigger_message = f"{self.script_key}: Something went wrong in monitoring due to {error}."
                self.sent_notification(last_signal_time=None)

    def start_monitoring_fixed(self):
        """ Checks whether the script was executed in defined time or not."""
        try:
            if self.is_active_time() and not self.stop_monitoring:
                last_signal_time = self.get_last_signal_time()
                if last_signal_time is not None:
                    last_run_date = datetime.fromtimestamp(last_signal_time).date()
                    today_date = datetime.today().date()
                    if last_run_date < today_date:
                        self.down_freq += 1
                        self.logger.critical(f"{self.script_key}: script did not run today.down_freq={self.down_freq} ")
                        self.trigger_message = f"Script did not run today. Last runtime: {self.to_readable_format(last_signal_time)}"
                    else:
                        self.logger.debug(f"{self.script_key}: script ran successfully today.")
                        return True
                else:
                    self.down_freq += 1
                    self.logger.critical(f"{self.script_key}: signal_time not found, down_freq={self.down_freq}.")
                    self.trigger_message = f"Signal_time not found. It should not be None."

                if self.down_freq >= self.max_down_freq:
                    # # send notification and stop monitoring it now.
                    self.stop_monitoring = True
                    self.trigger_time = time.time()
                    self.logger.info(f"{self.script_key}: script did not run today, triggering email.")
                    self.sent_notification(last_signal_time=last_signal_time)

            elif self.is_active_time() and self.stop_monitoring:
                #  if the notification was triggered yesterday, stop_monitoring again.
                if self.trigger_time is not None:
                    last_run_date = datetime.fromtimestamp(self.trigger_time).date()
                    today_date = datetime.today().date()
                    if last_run_date < today_date:
                        self.reset_monitor()
                    else:
                        self.logger.debug(f"{self.script_key}: script did not run today, Already notified.")

            else:
                self.logger.debug(f"{self.script_key}: Off time. active_hours {self.start_time}-{self.end_time}.")
        except Exception as error:
            self.logger.exception(f"{self.script_key}: Something went wrong in monitoring due to {error}.")
            self.down_freq += 1
            if self.down_freq >= self.max_down_freq:
                self.stop_monitoring = True
                self.trigger_time = time.time()
                self.trigger_message = f"{self.script_key}: Something went wrong in monitoring due to {error}."
                self.sent_notification(last_signal_time=None)

    def start_monitoring_server(self):
        try:
            if self.is_active_time():
                try:
                    response = requests.get(self.api_url, timeout=10)
                    if response.status_code == 503:
                        if not self.stop_monitoring:
                            self.down_freq += 1
                            self.logger.critical(f"{self.script_key}: Server was not started today. down_freq={self.down_freq}")
                            self.trigger_message = f"Server is running but was not started today."
                        else:
                            self.logger.debug(f"{self.script_key}: Server was not started today. down_freq={self.down_freq}")
                    elif (response.status_code!=200 and response.status_code!=503):
                        if not self.stop_monitoring:
                            self.down_freq += 1
                            self.logger.critical(f"{self.script_key}: Something is wrong with server, received status_code={response.status_code}. down_freq={self.down_freq}")
                            self.trigger_message = f"Something is wrong with server, received status_code={response.status_code}"
                        else:
                            self.logger.debug(f"{self.script_key}: Something is wrong with server, received status_code={response.status_code}. down_freq={self.down_freq}")
                    else:
                        self.logger.debug(f"{self.script_key}: Server is running fine.")
                        # start monitoring again after the server is UP.
                        if self.stop_monitoring:
                            self.reset_monitor()
                        return True
                except Exception as error:
                    if not self.stop_monitoring:
                        self.down_freq += 1
                        self.logger.critical(f"{self.script_key}: down_freq={self.down_freq}. Failed to get response due to {error}.")
                        self.trigger_message = f"Server is not responding. Failed to get response due to {error}."

                if self.down_freq >= self.max_down_freq and not self.stop_monitoring:
                    # # send notification and stop monitoring it now.
                    self.stop_monitoring = True
                    self.trigger_time = time.time()
                    self.logger.critical(f"{self.script_key}: Server is not running or didn't start today, triggering email. down_freq={self.down_freq}.")
                    self.sent_notification(last_signal_time=None)
                elif(self.stop_monitoring):
                    self.logger.debug(f"{self.script_key}: Already notified for failure.")
            else:
                self.logger.debug(f"{self.script_key}: Off time. active_hours {self.start_time}-{self.end_time}.")

        except Exception as error:
            self.logger.exception(f"{self.script_key}: Something went wrong in monitoring due to {error}.")
            self.down_freq += 1
            if self.down_freq >= self.max_down_freq:
                self.stop_monitoring = True
                self.trigger_time = time.time()
                self.trigger_message = f"{self.script_key}: Something went wrong in monitoring server due to {error}."
                self.sent_notification(last_signal_time=None)


    def reset_monitor(self):
        self.logger.info(f"Resetting monitoring for {self.script_key}.")
        self.stop_monitoring = False
        self.down_freq = 0

    def set_active_time(self):
        if self.monitor_type == MonitoringType.FIXED:
            self.start_time = dt_time(*map(int, self.active_time.split(':')))
            # buffer minutes.
            buffer_minutes = self.max_down_time/60
            self.end_time =  self.add_minutes_to_time(self.start_time, minutes_to_add=buffer_minutes)
        elif self.monitor_type == MonitoringType.CONTINUOUS or self.monitor_type == MonitoringType.SERVER:
            start_time, end_time = self.active_time.split("-")
            # Convert start_time and end_time to datetime.time objects
            self.start_time = dt_time(*map(int, start_time.split(':')))
            self.end_time = dt_time(*map(int, end_time.split(':')))

    @staticmethod
    def add_minutes_to_time(base_time, minutes_to_add):
        # Convert the base_time to a datetime.datetime object
        base_datetime = datetime.combine(datetime.today(), base_time)
        # Add the specified number of minutes
        updated_datetime = base_datetime + timedelta(minutes=minutes_to_add)
        # Extract the updated time component
        updated_time = updated_datetime.time()
        return updated_time
    def is_active_status(self):
        if not self.stop_monitoring and self.is_active_time():
            return True

    def is_active_time(self):
        # Get the current time
        current_time = datetime.now().time()
        return self.start_time <= current_time <= self.end_time

    @staticmethod
    def is_current_time_in_range(start_time, end_time):
        # Get the current time
        current_time = datetime.now().time()
        # Convert start_time and end_time to datetime.time objects
        start_time = dt_time(*map(int, start_time.split(':')))
        end_time = dt_time(*map(int, end_time.split(':')))
        # Check if the current time is within the specified range
        return start_time <= current_time <= end_time

    @staticmethod
    def to_readable_format(timestamp)->str:
        if isinstance(timestamp,(int,float)):
            dt_object = datetime.fromtimestamp(timestamp)
            # Format the datetime object as a readable string
            formatted_time = dt_object.strftime('%Y-%m-%d %H:%M:%S')
            return formatted_time
        elif(isinstance(timestamp, dt_time)):
            return timestamp.strftime('%H:%M')

    def sent_notification(self, last_signal_time=None):
        try:
            self.logger.info(f"{self.script_key}: Sending notification..")
            if self.monitor_type == MonitoringType.CONTINUOUS:
                subject = f"Jerusalem RT Dashboard (Hamal Zatal) - FAILURE: {self.script_name}"
                body = (f"<p><strong>{self.script_name}</strong> is not running. Please check the following details:</p>"
                        f"<ul>"
                        f"<li><strong>Last Beat Recorded: {self.to_readable_format(last_signal_time)}</strong></li>"
                        f"<li><strong>Active Monitoring Hours: {self.to_readable_format(self.start_time)} to {self.to_readable_format(self.end_time)}</strong></li>"
                        f"<li><strong>Max Down Time: {round(self.max_down_time/60,2)} minutes </strong>(Maximum allowed time difference between last beat and current time.)</li>"
                        f"<li><strong>Max Down Frequency: {self.max_down_freq} </strong>(No.of times downtime status was checked before sending email.)</li>"
                        f"<li><strong>Trigger Time: {self.to_readable_format(self.trigger_time)}</strong></li>"
                        f"</ul>"
                        f"<br><p><strong>*Code_Message:</strong> {self.trigger_message}</p>")
            elif self.monitor_type == MonitoringType.FIXED:
                subject = f"Jerusalem RT Dashboard (Hamal Zatal) - FAILURE: {self.script_name}"
                body = (
                    f"<p><strong>{self.script_name}</strong> did not run on specified time. Please check the following details:</p>"
                    f"<ul>"
                    f"<li><strong>Last Successful Run: {self.to_readable_format(last_signal_time)}</strong></li>"
                    f"<li><strong>Status Check Time: {self.to_readable_format(self.start_time)}</strong> (Expected time when the script should have run successfully.)</li>"
                    f"<li><strong>Max Down Frequency: {self.max_down_freq} </strong>(No.of times status was checked before sending email.)</li>"
                    f"<li><strong>Trigger Time: {self.to_readable_format(self.trigger_time)}</strong></li>"
                    f"</ul>"
                    f"<br><p><strong>*Code_Message:</strong> {self.trigger_message}</p>")

            elif self.monitor_type == MonitoringType.SERVER:
                subject = f"Jerusalem RT Dashboard (Hamal Zatal)-SERVER_FAILURE: {self.script_name}"
                body = (
                    f"<p><strong>{self.script_name}</strong> is not running. Please check the following details:</p>"
                    f"<ul>"
                    f"<li><strong>Active Monitoring Hours: {self.to_readable_format(self.start_time)} to {self.to_readable_format(self.end_time)}</strong></li>"
                    f"<li><strong>Max Down Frequency: {self.max_down_freq} </strong>(No.of times downtime status was checked before sending email.)</li>"
                    f"<li><strong>Trigger Time: {self.to_readable_format(self.trigger_time)}</strong></li>"
                    f"</ul>"
                    f"<br><p><strong>*Code_Message:</strong> {self.trigger_message}</p>")
            else:
                self.logger.critical(f"{self.script_key}: Template not defined for {self.monitor_type} monitor type")
                return None
            self.send_email_using_azure(logger=self.logger, subject=subject, body=body)
        except Exception as error:
            self.logger.exception(f"{self.script_key}: Something went wrong in sent_notification due to {error}.")

    @staticmethod
    def send_email_using_azure(logger, subject, body):
        try:
            logger.info(f"Sending Email with subject: {subject}")
            connection_string = "endpoint=https://alerts-service.unitedstates.communication.azure.com/;accesskey=qykQs0IbMg90yMO6iMSTICMRIKkx9XdQb6bYrNINURGZ7L7PmhuON55tfOpCko0dihx0C5KHmuc/JhC4/LXAZw=="
            client = EmailClient.from_connection_string(connection_string)
            message = {
                "senderAddress": "script-alerts@8ff5a4ab-94ae-4deb-bb18-3ceea8f4e6f4.azurecomm.net",
                "recipients": {
                    "to": RECIPIENTS,
                },
                "content": {
                    "subject": subject,
                    "plainText": body,
                    "html": body
                }
            }
            client.begin_send(message)
            logger.debug(f"Email sent.")
        except Exception as ex:
            logger.exception(f"Exception while sending email, {ex}")

    @staticmethod
    def get_status_dict(configuration, key_column="name"):
        status_dict = {}

        for script, config in configuration.items():
            key = config[key_column]
            status_dict[key] = None

        return status_dict

    @staticmethod
    def send_daily_update_email(logger, scripts_status_dict):
        logger.info("Sending daily update email.")
        success_color = "#00cc44;"
        failure_color = "#b30000;"
        subject = "Jerusalem RT Dashboard (Hamal Zatal) - Daily Update."
        status_tags = ""
        for script_name, status in scripts_status_dict.items():
            if status == True:
                tag = f'<li style="color:{success_color}"><strong>{script_name}: SUCCESS</strong></li>'
                status_tags+= tag
            else:
                tag = f'<li style="color:{failure_color}"><strong>{script_name}: FAILED</strong></li>'
                status_tags += tag
        body = (
            f"<p><strong>Hi,</strong></p>"
            f"<p><strong>Please check the status of following scripts:</strong></p>"
            f"<ul>"
            f"{status_tags}"
            f"</ul>")
        MonitorStatus.send_email_using_azure(logger, subject, body)

    @staticmethod
    def send_redis_update_email(logger):
        logger.info("Sending Redis update email.")
        subject = f"Jerusalem RT Dashboard (Hamal Zatal) - FAILURE: REDIS"""
        body = (f"<p><strong>REDIS SERVER</strong> is not running.</p>"
                f"<p>It must be running in order to continue scripts' monitoring.</p>")
        MonitorStatus.send_email_using_azure(logger, subject, body)


if __name__ == "__main__":
    try:
        log_file_init = "logs_monitoring_"
        logger = logger_setup(console_log_level=console_logging_level, file_log_level=file_logging_level,
                              file_init=log_file_init, date_format='%Y-%m-%d')
        last_logger_setup_date = datetime.now().date()
        last_email_date = None
        send_redis_email = True

        logger.info(f"********************************************************************")
        logger.info(f"********************************Start********************************")
        logger.info(f"********************************************************************")
        # Delete previous log files older than max_days
        delete_old_log_files(MAX_LOGS_DAYS, logger, file_init=log_file_init, date_format='%Y-%m-%d')
        all_monitoring_classes = []

        DAILY_NOTIFICATION_TIME = datetime.strptime(DAILY_NOTIFICATION_TIME, "%H:%M").time()
        STATUS_DICT = MonitorStatus.get_status_dict(configuration, key_column="name")

        for script, config in configuration.items():
            logger.info(f"Loading Monitoring configuration for: {script}")
            perfm_monitor = MonitorStatus(logger=logger, script_key=config["key"], script_name=config["name"],
                                          monitor_type=config["monitor_type"],
                                          active_time=config["active_time"], max_down_time=config.get("max_down_time", 1),
                                          max_down_freq=config.get("max_down_freq", 3), api_url=config.get("api_url"),
                                          host='localhost', port=6379, timeout=20, expiry=None)
            all_monitoring_classes.append(perfm_monitor)

        while True:
            # check redis status
            is_redis_active = all_monitoring_classes[0].check_redis_status()
            if is_redis_active:
                for monitor_class in all_monitoring_classes:
                    is_success = monitor_class.start_monitoring()
                    STATUS_DICT[monitor_class.script_name] = is_success
                send_redis_email = True
            else:
                # redis not active.
                if send_redis_email:
                    all_monitoring_classes[0].send_redis_update_email(logger)
                    # avoid sending email again.
                    send_redis_email = False
                else:
                    logger.debug("Unable to access redis. Already notified.")

            # close previous logger and start new date's logger.
            current_date = datetime.now().date()
            if current_date > last_logger_setup_date:
                logger.info(f"********************************************************************")
                logger.info(f"********************************END********************************")
                logger.info(f"********************************************************************")
                for handler in logger.handlers[:]:
                    logger.removeHandler(handler)
                # Reconfigure logger for a new day
                print(f"Starting New Logger for: {current_date.strftime('%Y-%m-%d')}")
                logger = logger_setup(console_log_level=console_logging_level, file_log_level=file_logging_level,
                                      file_init=log_file_init, date_format='%Y-%m-%d')
                for monitor_class in all_monitoring_classes:
                    monitor_class.update_logger(logger=logger)

                logger.info(f"********************************************************************")
                logger.info(f"********************************Start********************************")
                logger.info(f"********************************************************************")
                last_logger_setup_date = current_date

            # send daily update email.
            if SEND_DAILY_EMAIL and (last_email_date is None or current_date > last_email_date):
                if datetime.now().time() >= DAILY_NOTIFICATION_TIME:
                    # trigger a daily email, with the status of the scripts. Color scripts with their status,
                    # and then keep optional paramter to avoid sending email again.
                    MonitorStatus.send_daily_update_email(logger, scripts_status_dict=STATUS_DICT)
                    last_email_date = datetime.now().date()

            time.sleep(MONITORING_FREQUENCY*60)

    except Exception as e:
        # Log the error or perform any necessary actions
        logger.exception(f"Error occurred: {str(e)}")
        sys.exit(1)

sys.exit(0)