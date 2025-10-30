import csv
import os
import threading
from datetime import datetime, UTC


# Lock for thread-safe CSV file writing
_csv_lock = threading.Lock()


def save_to_csv(data, csv_filename, receive_time=None):

    if receive_time is None:
        receive_time = datetime.now(UTC).isoformat()
    
    # Add the receiving time to the data
    data_with_receive_time = data.copy()
    data_with_receive_time['receive_time'] = receive_time
    
    # Use lock to ensure thread-safe file writing
    with _csv_lock:
        file_exists = os.path.exists(csv_filename)
        
        with open(csv_filename, 'a', newline='') as csvfile:
            fieldnames = ['device_id', 'timestamp', 'protocol', 'sensor', 'value', 'receive_time']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header if file is new
            if not file_exists:
                writer.writeheader()
            
            # Write the data row
            writer.writerow(data_with_receive_time)
        
        print(f"[STORAGE] Saved data to {csv_filename}: {data_with_receive_time}")

