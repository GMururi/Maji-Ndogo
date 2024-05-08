import pandas as pd
import logging 

from data_ingestion import create_db_engine, query_data, read_from_web_CSV

class FieldDataProcessor:
    """Processes field data based on configuration parameters.

    This class provides methods to ingest data from an SQL database, rename columns, apply corrections, and merge weather station data to the main DataFrame.

    Args:
        logging_level (str, optional): The logging level. Defaults to "INFO".

    Attributes:
        db_path (str): The path to the database.
        sql_query (str): The SQL query to execute.
        columns_to_rename (dict): A dictionary mapping columns to rename.
        values_to_rename (dict): A dictionary mapping values to rename.
        weather_map_data (str): The URL of the weather mapping CSV file.
        logger (logging.Logger): The logger object.
        df (DataFrame): The DataFrame containing the field data.
        engine (object): The engine object for the database.

    Methods:
        initialize_logging(logging_level): Initializes logging for the instance.
        ingest_sql_data(): Ingests data from an SQL database.
        rename_columns(): Renames columns in the DataFrame.
        apply_corrections(): Corrects crop strings in the DataFrame.
        weather_station_mapping(): Merges weather station data to the DataFrame.
        process(): Executes the processing pipeline.

    """
    def __init__(self, config_params, logging_level="INFO"):  # Make sure to add this line, passing in config_params to the class 
        """
        Initialize the FieldDataProcessor.

        Parameters:
        - config_params (dict): Configuration parameters including 'db_path', 'sql_query', 'columns_to_rename',
                               'values_to_rename', and 'weather_mapping_csv'.
        - logging_level (str): Logging level, default is "INFO".
        """
        self.db_path = config_params['db_path']
        self.sql_query = config_params['sql_query']
        self.columns_to_rename = config_params['columns_to_rename']
        self.values_to_rename = config_params['values_to_rename']
        self.weather_map_data = config_params['weather_mapping_csv']

        # Add the rest of your class code here        
        self.initialize_logging(logging_level)

        # We create empty objects to store the DataFrame and engine in
        self.df = None
        self.engine = None
        
    # This method enables logging in the class.
    def initialize_logging(self, logging_level):
        """
        Sets up logging for this instance of FieldDataProcessor.
        """
        logger_name = __name__ + ".FieldDataProcessor"
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

        # Use self.logger.info(), self.logger.debug(), etc.

    # let's focus only on this part from now on
    def ingest_sql_data(self):
        """
        Loads data from the database using the provided SQL query and stores it in a Pandas DataFrame.

        Returns:
            pd.DataFrame: The loaded and processed DataFrame.
        """
        self.engine = create_db_engine(self.db_path)
        self.df = query_data(self.engine, self.sql_query)
        self.logger.info("Sucessfully loaded data.")
        return self.df
        
    def rename_columns(self):
        """
        Swaps column names based on the configuration dictionary.

        Raises:
            ValueError: If any of the columns to rename are not present in the DataFrame.
        """
       # Extract the columns to rename from the configuration
        column1, column2 = list(self.columns_to_rename.keys())[0], list(self.columns_to_rename.values())[0]

         # Temporarily rename one of the columns to avoid a naming conflict
        temp_name = "__temp_name_for_swap__"
        while temp_name in self.df.columns:
            temp_name += "_"

        # Perform the swap
        self.df = self.df.rename(columns={column1: temp_name, column2: column1})
        self.df = self.df.rename(columns={temp_name: column2})             
       
        self.logger.info(f"Swapped columns: {column1} with {column2}")
        
    def apply_corrections(self, column_name='Crop_type', abs_column='Elevation'):
        """
        Applies data corrections to the DataFrame, including:

        * Taking absolute values of a specified column (e.g., Elevation).
        * Renaming values in a specific column based on a provided dictionary.

        Args:
            column_name (str, optional): Name of the column to apply value renaming to. Defaults to 'Crop_type'.
            abs_column (str, optional): Name of the column to take absolute values of. Defaults to 'Elevation'.
        """
        self.df['Elevation'] = self.df['Elevation'].abs()
        self.df['Crop_type'] = self.df['Crop_type'].apply(lambda crop: self.values_to_rename.get(crop, crop))       

    def weather_station_mapping(self):
        """
        Merges weather station data from a CSV file with the main DataFrame based on a common field ID.

        Returns:
            pd.DataFrame: The processed DataFrame with merged weather station data.
        """
        # Merge the weather station data to the main DataFrame
        self.df = pd.merge(self.df, read_from_web_CSV(self.weather_map_data), on='Field_ID', how='left')
        self.df = self.df.drop(columns="Unnamed: 0")
        return self.df

    def process(self):
        """
        Executes the processing pipeline:
           1. Loads data from the database.
           2. Renames columns.
           3. Applies data corrections.
           4. Maps weather station data.

        Returns:
            pd.DataFrame: The processed and cleaned DataFrame.
        """
        self.ingest_sql_data()
        #Insert your code here
        self.rename_columns()
        self.apply_corrections()
        self.weather_station_mapping()   
