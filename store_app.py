from flask import Flask, request, jsonify, send_file
import pymongo
import pandas as pd
import random
import string
import datetime

app = Flask(__name__)

# MongoDB connection
client = pymongo.MongoClient('localhost', 27017)  # Replace 'localhost' and '27017' with your MongoDB connection details
db = client['store_data']  # Replace 'store_data' with your preferred database name
status_db = client['report_status']  # Replace 'report_status' with the name of your status tracking database

#function to generate a random string as the report_id
def generate_report_id(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


@app.route('/trigger_report', methods=['POST'])
def trigger_report():
    # Get the current timestamp to be used as the max timestamp among all observations
    max_timestamp_utc = db['store_activity'].find_one(sort=[('timestamp_utc', -1)])['timestamp_utc']
    
    # Generate a random report_id
    report_id = generate_report_id()
    
    # Start the report generation asynchronously (you can use background tasks or queues for this in production)
    generate_report_background(report_id, max_timestamp_utc)
    
    # Save the report_id and initial status ('running') in the report_status_collection
    status_collection = status_db['report_status_collection']
    status_collection.insert_one({'report_id': report_id, 'status': 'running'})
    
    return jsonify({'report_id': report_id})


#function to generate the report in the background
def generate_report_background(report_id, max_timestamp_utc):
    try:
        # Fetch data from MongoDB collections and perform necessary data processing and interpolation
        store_activity_collection = db['store_activity']
        store_business_hours_collection = db['store_business_hours']
        store_timezone_collection = db['store_timezone']

        # Data structures to store processed data
        store_uptime = {}
        store_downtime = {}

        # Loop through store activity data
        for activity in store_activity_collection.find():
            store_id = activity['store_id']
            timestamp_utc = activity['timestamp_utc']
            status = activity['status']

            # Convert the timestamp to a local time based on the store's timezone
            timezone_str = store_timezone_collection.find_one({'store_id': store_id})['timezone_str']
            tz = timezone(timezone_str)
            local_time = datetime.fromtimestamp(timestamp_utc, tz)

            # Get the business hours for the store on the day of the observation
            day_of_week = local_time.weekday()
            business_hours = store_business_hours_collection.find_one({'store_id': store_id, 'dayOfWeek': day_of_week})

            if business_hours:
                start_time_local = datetime.strptime(business_hours['start_time_local'], '%H:%M:%S')
                end_time_local = datetime.strptime(business_hours['end_time_local'], '%H:%M:%S')

                # Ensure the timestamp is within business hours
                if start_time_local.time() <= local_time.time() <= end_time_local.time():
                    store_uptime[store_id] = store_uptime.get(store_id, 0) + (status == 'active')
                    store_downtime[store_id] = store_downtime.get(store_id, 0) + (status == 'inactive')

        # Once the report is ready, save it to a CSV file
        if not store_uptime:
            raise Exception("No data available for report generation.")

        report_data = {
            'store_id': list(store_uptime.keys()),
            'uptime_last_hour': list(store_uptime.values()),
            'uptime_last_day': [v * 24 for v in store_uptime.values()],
            'uptime_last_week': [v * 24 * 7 for v in store_uptime.values()],
            'downtime_last_hour': list(store_downtime.values()),
            'downtime_last_day': [v * 24 for v in store_downtime.values()],
            'downtime_last_week': [v * 24 * 7 for v in store_downtime.values()],
        }

        df_report = pd.DataFrame(report_data)
        csv_file_path = f'./reports/{report_id}.csv'  # Replace './reports/' with the appropriate path to your CSV files
        df_report.to_csv(csv_file_path, index=False)

        # Update the report status to 'complete' in the report_status_collection
        status_collection = status_db['report_status_collection']
        status_collection.update_one({'report_id': report_id}, {'$set': {'status': 'complete'}})
        print(f"Report generation for report_id={report_id} is complete.")

    except Exception as e:
        # If there was an error during report generation, update the status to 'error' in the report_status_collection
        status_collection = status_db['report_status_collection']
        status_collection.update_one({'report_id': report_id}, {'$set': {'status': 'error'}})
        print(f"Error occurred during report generation for report_id={report_id}: {str(e)}")


@app.route('/get_report', methods=['GET'])
def get_report():
    report_id = request.args.get('report_id')

    # Check if the report generation is complete
    if is_report_complete(report_id):
        # Get the report status from the report_status_collection
        status_collection = status_db['report_status_collection']
        report_status = status_collection.find_one({'report_id': report_id})

        if report_status['status'] == 'complete':
            csv_file_path = f'./reports/{report_id}.csv'  # Replace './reports/' with the appropriate path to your CSV files
            return send_file(csv_file_path, as_attachment=True, attachment_filename='report.csv')

        elif report_status['status'] == 'running':
            return jsonify({'status': 'Running'})

        elif report_status['status'] == 'error':
            return jsonify({'status': 'Error during report generation.'})

    # If report_id is invalid or the report generation is not complete, return "Invalid Report ID"
    return jsonify({'status': 'Invalid Report ID'})


def is_report_complete(report_id):
    # Check if the report generation is complete in the MongoDB status collection
    status_collection = status_db['report_status_collection']

    report_status = status_collection.find_one({'report_id': report_id})
    if report_status and report_status.get('status') == 'complete':
        return True
    else:
        return False


if __name__ == '__main__':
    app.run(debug=True)
