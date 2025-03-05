import pandas as pd
from sklearn.preprocessing import MinMaxScaler

class DataPreprocessor:
    def __init__(self, file_path):
        self.file_path = file_path
    
    def load_data(self):
        df = pd.read_csv(self.file_path)
        df['DateTime'] = pd.to_datetime(df['DateTime'])
        return df
    
    def preprocess_data(self, df):

        df = df.drop(columns = ['ID'])

        df['Hour'] = df['DateTime'].dt.hour
        df['Day'] = df['DateTime'].dt.day
        df['Month'] = df['DateTime'].dt.month
        df['Weekday'] = df['DateTime'].dt.weekday

        scaler = MinMaxScaler()
        df['Vehicles'] = scaler.fit_transform(df[['Vehicles']])

        return df
    
if __name__ == "__main__":
    preprocessor = DataPreprocessor("C:/Users/iamwa/Desktop/adv-ai-project/advanced-traffic-signal-control-system/data/traffic.csv")
    data = preprocessor.load_data()
    preprocessed_data = preprocessor.preprocess_data(data)
    print(preprocessed_data.head())
