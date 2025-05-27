import os
import json
from datetime import datetime

dag_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(dag_dir, "latest_job_date.json")

# find the value of the key end_date in the json file if the key exists, if not return today's date
def get_end_date():
    if os.path.exists(log_file):
        with open(log_file, "r") as f:
            data = json.load(f)
            if "end_date" in data:
                return data["end_date"]
    return datetime.now().date()


print(get_end_date())