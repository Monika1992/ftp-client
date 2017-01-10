from Configuration import Configuration
from Logger import Logger
from Mail_Client import Mail_client
from Correction_Downloader import Correction_downloader
from ftplib import *
from datetime import *


# Class responsible for FTP work - download, upload, creating connection, list of download data preparation
class FTP_client(object):

    def __init__(self, configuration):

        self.c_d = Correction_downloader()

        self.today = configuration.TODAY
        self.ftp_address = configuration.FTP_ADRESS
        self.ftp_name = configuration.FTP_NAME
        self.ftp_password = configuration.FTP_PASSWORD
        self.ftp_base_dir = configuration.FTP_BASE_DIR
        self.ftp_base_dir_2 = configuration.FTP_BASE_DIR_2
        self.models_list = configuration.MODELS
        self.parameters = configuration.PARAMETERS
        self.parameters_2 = configuration.PARAMS_2
        self.download_dir = configuration.DOWNLOAD_DIR
        self.logger_file = configuration.LOGGER_FILE
        self.error_messages = []

        self.run_download()
        self.run_download_2()

    # create ftp connection object
    def connect_to_ftp(self):
        try:
            ftp_connection = FTP(self.ftp_address, self.ftp_name, self.ftp_password)
            print "Connected to FTP"
            return ftp_connection
        except Exception as e:
            print "Connecting to FTP failed with exception: " + str(e)

    # close ftp connection object
    def disconnect_from_ftp(self, ftp_connection):
        try:
            ftp_connection.quit()
            print "Disconnected from FTP"
        except Exception as e:
            print "Disconnecting from ftp failed with exception: " + str(e)
        pass

    # return dates of prediction download dates depending on processed model
    def get_download_dates(self, start_date, model):
        predicted_days_dates = []

        if model == 'gum':
            predicted_days = 4
        elif model == 'arpege':
            predicted_days = 3
        else:
            predicted_days = 9

        for i in range(1, predicted_days + 1):
            date = start_date + timedelta(days=i)
            predicted_days_dates.append(str(date).replace('-', '_'))

        print "For model " + str(model) + " we are going to download dates: " +str(predicted_days_dates)
        return predicted_days_dates

    # creates concrete FTP file path for downloading data
    def create_download_dir_string(self, model):
        if model == 'gfs':
            download_dir_string = self.ftp_base_dir
        else:
            download_dir_string = self.ftp_base_dir + '_' + model.upper()

        return download_dir_string

    # quick FTP data download
    def get_data_from_ftp(self, connection, ftp_address, local_address, file_name):
        connection.retrbinary('RETR %s' % ftp_address + '/' + file_name, open(local_address, 'wb').write)

    # upload data to FTP dir
    def send_data_to_ftp(self, connection, local_file_path, ftp_file_path):
        pass

    def create_set_of_file_names(self, model):
        model_file_names = []
        dates = self.get_download_dates(self.today, model)
        for parameter in self.parameters:
            for date in dates:
                if model == 'gfs':
                    file_name = 'p' + str(parameter) + str(date) + '.tif'
                    model_file_names.append(file_name)
                else:
                    file_name = 'p_' + str(model) + str(parameter) + str(date) + '.tif'
                    model_file_names.append(file_name)
        return model_file_names

    def create_set_of_file_names_2(self, model):
        model_file_names = []
        for parameter in self.parameters_2:
            if model =='gfs':
                file_name = 'p' + str(parameter) + '.tif'
                model_file_names.append(file_name)
            else:
                file_name = 'p_' + str(model) + str(parameter) + '.tif'
                model_file_names.append(file_name)
        return model_file_names

    def create_download_dir_string_2(self, model, date):
        if model == 'gfs':
            download_dir_string = self.ftp_base_dir_2 + '/' + date + '/'
        else:
            download_dir_string = self.ftp_base_dir_2 + '_' + model.upper() + '/' + date

        return download_dir_string

    def run_download(self):
        config = Configuration()
        logger = Logger(config)
        mail_client = Mail_client(config)

        for model in self.models_list:
            connection = self.connect_to_ftp()
            ftp_data_path = self.create_download_dir_string(model)
            model_file_names = self.create_set_of_file_names(model)

            for file_name in model_file_names:
                try:
                    if model == 'gfs':
                        local_file_name = file_name[:2] + 'gfs_' + file_name[2:]
                        self.get_data_from_ftp(connection, ftp_data_path, self.download_dir + str(model.upper()) + '/' + local_file_name, file_name)
                    else:
                        self.get_data_from_ftp(connection, ftp_data_path, self.download_dir + str(model.upper()) + '/' + file_name, file_name)
                except Exception as e:
                    error_message = "Downloading dataset " + str(file_name) + " failed with: " + str(e)
                    self.c_d.append_line_to_correction_file(config.CORRECTION_FILE, str(file_name))
                    self.error_messages.append(logger.create_message_text(error_message))
                    print error_message
                    continue
            self.disconnect_from_ftp(connection)

        logger.add_log_message(self.error_messages)
        if logger.check_if_log_file_is_not_empty(self.logger_file):
            mail_client.send_mail(config.SENDER_MAIL_ADDRESS, config.SENDER_MAIL_PASSWORD, config.RECEIVER_MAIL_ADDRESS,
                                  config.MAIL_SUBJECT, config.MAIL_MESSAGE_BASE, self.logger_file, config.MAIL_SERVER,
                                  config.MAIL_PORT)
            logger.clear_log_file(self.logger_file)
        else:
            print "There is nothing to send via mail!"

    def run_download_2(self):
        config = Configuration()
        logger = Logger(config)
        mail_client = Mail_client(config)

        for model in self.models_list:
            connection = self.connect_to_ftp()
            dates = self.get_download_dates(config.TODAY, model)
            for date in dates:
                ftp_data_path = self.create_download_dir_string_2(model, date)

                model_file_names = self.create_set_of_file_names_2(model)

                for file_name in model_file_names:
                    try:
                        if model == 'gfs':
                            local_file_name = file_name[:2] + 'gfs_' + file_name[2:-4] + '_' + str(date) + '.tif'
                            self.get_data_from_ftp(connection, ftp_data_path, self.download_dir + str(model.upper()) + '/' + local_file_name, file_name)
                        else:
                            local_file_name = file_name[:-4] + '_' + str(date) + '.tif'
                            self.get_data_from_ftp(connection, ftp_data_path, self.download_dir + str(model.upper()) + '/' + local_file_name, file_name)
                    except Exception as e:
                        error_message = "Downloading dataset " + str(file_name) + " failed with: " + str(e)
                        self.c_d.append_line_to_correction_file(config.CORRECTION_FILE, str(file_name))
                        self.error_messages.append(logger.create_message_text(error_message))
                        print error_message
                        continue
            self.disconnect_from_ftp(connection)

        logger.add_log_message(self.error_messages)
        if logger.check_if_log_file_is_not_empty(self.logger_file):
            mail_client.send_mail(config.SENDER_MAIL_ADDRESS, config.SENDER_MAIL_PASSWORD, config.RECEIVER_MAIL_ADDRESS, config.MAIL_SUBJECT, config.MAIL_MESSAGE_BASE, self.logger_file, config.MAIL_SERVER, config.MAIL_PORT)
            logger.clear_log_file(self.logger_file)
        else:
            print "There is nothing to send via mail!"

