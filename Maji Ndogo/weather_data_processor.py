import re
import numpy as np
import pandas as pd
import logging
from data_ingestion import read_from_web_CSV

 

class WeatherDataProcessor:
    """Processes weather data based on configuration parameters.

    Args:
        config_params (dict): A dictionary containing configuration parameters.
        logging_level (str, optional): The logging level. Defaults to "INFO".

    Attributes:
        weather_station_data (str): The path to the weather CSV file.
        patterns (dict): A dictionary mapping measurement keys to regex patterns.
        weather_df (DataFrame): The DataFrame containing weather data.
        logger (logging.Logger): The logger object.

    Methods:
        initialize_logging(logging_level): Initializes logging for the instance.
        weather_station_mapping(): Maps weather station data to the DataFrame.
        extract_measurement(message): Extracts a measurement from a message using regex patterns.
        process_messages(): Processes messages to extract measurements.
        calculate_means(): Calculates the mean values of measurements.
        process(): Executes the processing pipeline.

    """
    def __init__(self, config_params, logging_level="INFO"): # Now we're passing in the confi_params dictionary already
        """
        Initializes the WeatherDataProcessor class.

        Args:
            config_params (dict): Dictionary containing configuration parameters.
                * weather_csv_path (str): Path to the weather station data CSV file.
                * regex_patterns (dict): Dictionary of measurement types as keys and regex patterns as values.
            logging_level (str, optional): Logging level for the class. Defaults to "INFO".
        """
        self.weather_station_data = config_params['weather_csv_path']
        self.patterns = config_params['regex_patterns']
        self.weather_df = None  # Initialize weather_df as None or as an empty DataFrame
        self.initialize_logging(logging_level)

    def initialize_logging(self, logging_level):
        """
        Sets up logging for the class.

        Args:
            logging_level (str): Logging level for the class.
        """
        logger_name = __name__ + ".WeatherDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False  # Prevents log messages from being propagated to the root logger

        # Set logging level
        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":  # Option to disable logging
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO  # Default to INFO

        self.logger.setLevel(log_level)

        # Only add handler if not already added to avoid duplicate messages
        if not self.logger.handlers:
            ch = logging.StreamHandler()  # Create console handler
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def weather_station_mapping(self):
        """
        Loads weather station data from the web and performs initial transformations (if needed).
        """
        self.weather_df = read_from_web_CSV(self.weather_station_data)
        self.logger.info("Successfully loaded weather station data from the web.") 
        # Here, you can apply any initial transformations to self.weather_df if necessary.

    
    def extract_measurement(self, message):
        """
        Extracts measurement type and value from a message based on defined patterns.

        Args:
            message (str): Message string to extract measurements from.

        Returns:
            tuple: A tuple containing (measurement_type, value) or (None, None) if no match found.
        """
        for key, pattern in self.patterns.items():
            match = re.search(pattern, message)
            if match:
                self.logger.debug(f"Measurement extracted: {key}")
                return key, float(next((x for x in match.groups() if x is not None)))
        self.logger.debug("No measurement match found.")
        return None, None

    def process_messages(self):
        """
        Processes messages in the DataFrame to extract measurements and add them as new columns.

        Raises:
            ValueError: If `weather_df` is not initialized.
        """
        if self.weather_df is not None:
            result = self.weather_df['Message'].apply(self.extract_measurement)
            self.weather_df['Measurement'], self.weather_df['Value'] = zip(*result)
            self.logger.info("Messages processed and measurements extracted.")
        else:
            self.logger.warning("weather_df is not initialized, skipping message processing.")
        return self.weather_df

    def calculate_means(self):
        """
        Calculates mean values for each measurement type at each weather station.

        Raises:
            ValueError: If `weather_df` is not initialized.
        """
        if self.weather_df is not None:
            means = self.weather_df.groupby(by=['Weather_station_ID', 'Measurement'])['Value'].mean()
            self.logger.info("Mean values calculated.")
            return means.unstack()
        else:
            self.logger.warning("weather_df is not initialized, cannot calculate means.")
            return None
    
    def process(self):
        """
        Executes the processing pipeline: load data, extract measurements, calculate means.

        Logs completion message and returns the processed DataFrame.
        """
        self.weather_station_mapping()  # Load and assign data to weather_df
        self.process_messages()  # Process messages to extract measurements
        self.logger.info("Data processing completed.")


