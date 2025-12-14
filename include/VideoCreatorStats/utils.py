import pandas as pd 
import logging

class IngestionFile:
    """ Wrapper class for ingesting and cleaning a file.

        This class is used to load and clean a file. It is initialized with a filename and a QA pipeline.
        The QA pipeline is a list of dictionaries, each containing a column name, a type, and a list of assertions.
        The assertions are used to validate the data.
    """
    def __init__(self, qa_pipeline=[], filename="", header_rows=0):
        self.filename = filename
        self.qa_pipeline = qa_pipeline
        self.header_rows = header_rows
        self.header_types = {qa['col']: qa['type'] for qa in qa_pipeline}
        self.data = None
        
    def load_data(self):
        """ Loads the data from the file. """
        self.data = pd.read_csv(self.filename, header=self.header_rows, dtype=self.header_types)
        return self.data

    def clean_data(self):
        """ Runs QA checks on the data and returns the cleaned data. """
        if self.data is None:
            self.load_data()
        df = self.data
        filename = self.filename
        for qa in self.qa_pipeline:
            col = qa['col']
            
            # NA Handling
            if "fill_na" in qa:
                df[col] = df[col].fillna(qa['fill_na'])
            else:
                # Error if there are any na values.
                if df[col].isnull().any():
                    raise logging.error(f"{filename} - Column {col} contains null values.")

            # Warnings
            if "warnings" in qa:
                for warning in qa["warnings"]:
                    if warning["condition"](df):
                        logging.warning(f"{filename} - {warning['message']}")
            # Assertions
            if "assertions" in qa:
                for assertion in qa["assertions"]:
                    if assertion["condition"](df):
                        logging.error(f"{filename} - {assertion['message']}")
                        if assertion["should_fail"]:
                            raise ValueError(f"{filename} - {assertion['message']}")
            # Post-Processing
            if "post_processing" in qa:
                for post_processing in qa["post_processing"]:
                    df[col] = post_processing(df[col])
        self.data = df
        return df

