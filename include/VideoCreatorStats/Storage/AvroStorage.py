from .BaseStorage import BaseStorage
import fastavro
import pandas as pd

class AvroPandasStorage(BaseStorage):
    def __init__(self, path: str, data: pd.DataFrame):
        super().__init__()
        self.path = path
        self.data = data
    
    def save(self):
        """
        Helper method to save a pandas DataFrame to an Avro file.
        Infers schema from DataFrame dtypes.

        Args:
            df (pd.DataFrame): DataFrame to save.
            path (str): Destination file path.
        """
        df = self.data
        df_clean = df.where(pd.notnull(df), None)
        
        records = df_clean.to_dict('records')
        
        schema = {
            'doc': 'Auto generated schema',
            'name': 'Data',
            'namespace': 'test',
            'type': 'record',
            'fields': []
        }
        
        for col, dtype in df.dtypes.items():
            avro_type = 'string'
            dtype_str = str(dtype).lower()
            if 'int' in dtype_str:
                avro_type = ['null', 'long']
            elif 'float' in dtype_str:
                avro_type = ['null', 'double']
            elif 'bool' in dtype_str:
                avro_type = ['null', 'boolean']
            elif 'datetime' in dtype_str:
                avro_type = ['null', {'type': 'long', 'logicalType': 'timestamp-micros'}]
            elif 'object' in str(dtype):
                val = df_clean[col].dropna().iloc[0] if not df_clean[col].dropna().empty else None
                if isinstance(val, list):
                    avro_type = {'type': 'array', 'items': 'string'}
                else:
                    avro_type = ['null', 'string']
            
            schema['fields'].append({'name': col, 'type': avro_type})
            
        with open(self.path, 'wb') as out:
            fastavro.writer(out, schema, records)
        