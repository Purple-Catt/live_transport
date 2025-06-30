from gzip import GzipFile
from io import BytesIO
import os
from datetime import datetime as dt, timedelta
import zmq
from ctx import ctx_decode
import pandas as pd

first = True
lines = ['1', '2', '17']
start = dt.strptime("06:00", "%H:%M").time()
end = dt.strptime("23:59", "%H:%M").time()
# Activating connection to receiving data
context = zmq.Context()
subscriber = context.socket(zmq.SUB)
subscriber.connect("tcp://pubsub.besteffort.ndovloket.nl:7817")
subscriber.setsockopt_string(zmq.SUBSCRIBE, "/GOVI/KV8")

# Run only between 6:00am and 11:59pm, also to avoid timestamps' problems in data after midnight
while True:
    if start < dt.now().time() < end:
        multipart = subscriber.recv_multipart()
        address = multipart[0]
        contents = b''.join(multipart[1:])
        # Try retrieving data
        try:
            contents = GzipFile('', 'r', 0, BytesIO(contents)).read()
            str_contents = contents.decode('utf-8')  # All data encoded in CTX
            splitted_content = str_contents.split('\n')
            if splitted_content[3].startswith('GVB'):
                if splitted_content[1].startswith(r'\TDATEDPASSTIME'):
                    # Keep a dataframe updated with real time data of vehicles driving predefined lines managed by GVB
                    if first:
                        df = ctx_decode(str_contents)
                        df.drop(index=df.loc[df['RecordedArrivalTime'] == r'\0'].index,
                                inplace=True)
                        df = df[df['LinePublicNumber'].isin(lines)]

                        if df.duplicated(subset=['VehicleNumber']).any():
                            df.sort_values(by='RecordedArrivalTime',
                                           inplace=True,
                                           kind='mergesort',
                                           key=lambda x: pd.to_datetime(x))
                            df.sort_values(by='VehicleNumber',
                                           inplace=True,
                                           kind='mergesort')
                            df.drop_duplicates(subset=['VehicleNumber'],
                                               keep='last',
                                               inplace=True)

                        if not df.empty:
                            df.set_index('VehicleNumber', inplace=True)
                            first = False

                    else:
                        temp = ctx_decode(str_contents)
                        temp.drop(index=temp.loc[temp['RecordedArrivalTime'] == r'\0'].index,
                                  inplace=True)
                        temp = temp[temp['LinePublicNumber'].isin(lines)]

                        if temp.duplicated(subset=['VehicleNumber']).any():
                            temp.sort_values(by='RecordedArrivalTime',
                                             inplace=True,
                                             kind='mergesort',
                                             key=lambda x: pd.to_datetime(x))
                            temp.sort_values(by='VehicleNumber',
                                             inplace=True,
                                             kind='mergesort')
                            temp.drop_duplicates(subset=['VehicleNumber'],
                                                 keep='last',
                                                 inplace=True)

                        if not temp.empty:
                            temp.set_index('VehicleNumber', inplace=True)
                            df.update(temp)

                            df = pd.concat([df, temp[~temp.index.isin(df.index)]])
                            # Drop vehicles that haven't been updated for more than 10 mins
                            for idx in df.index:
                                if dt.now() - dt.combine(dt.today().date(),
                                                         dt.strptime(df.loc[idx, 'RecordedArrivalTime'],
                                                                     '%H:%M:%S').time()) > timedelta(minutes=10):
                                    df.drop(index=idx, inplace=True)

                            os.system('cls')
                            print(df.loc[:, ['LinePublicNumber', 'RecordedArrivalTime', 'RecordedDepartureTime',
                                             'QuayCode', 'DestinationName', 'LineDirection', 'TripStopStatus',
                                             'JourneyStopType']])

                else:
                    print(str_contents)
        except:
            raise ConnectionError('Failed to connect. Try again')
