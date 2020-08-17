from flask import Flask, request
import redis
import time
import datetime
import json

app = Flask(__name__)
db = redis.Redis(host='redis', port=6379)

def get_current_time():
    timestamp = datetime.datetime.utcnow()
    return "[UTC] " + timestamp.strftime("%Y-%m-%d %H:%M")


def create_response(success = True, response = None, error_message = None, data = {}):
    return {
        "success": success,
        "error_message": error_message, 
        "response": response,
        "data": data,
    }

def validate_request(valid_parameters, received_parameters):
    if(valid_parameters != received_parameters):
        return create_response(
            success=False,
            error_message=f"Got invalid arguments. The valid arguments for this endpoint are: {valid_parameters}"
        )

def get_all_jobs():
    valid_ids = db.get("id")
    if valid_ids is None:
        return {}
    valid_ids = int(valid_ids)
    result = {}
    for id in range(1, valid_ids + 1):
        json_string = db.get(id)
        result[id] = json.loads(json_string)
    return result

def get_jobs_by_status(status):
    jobs = get_all_jobs()
    return {id:job for id,job in jobs.items() if job["status"] == status}

def validate_id_request(data):
    parameter_error = validate_request(['id'], list(data.keys()))
    if (parameter_error != None):
        return parameter_error
    job_id = data["id"]
    if(db.exists(job_id) == False):
        return create_response(success=False, error_message=f"No job with job ID {job_id}.")

@app.route("/")
def home():
    return create_response(success=True, response="Welcome to Anirudh's Atlan Collect server.")

@app.route("/create", methods=['POST'])
def create_job():
    data = request.form.to_dict()
    request_error = validate_request(['name'], list(data.keys()))
    if (request_error != None):
        return request_error
    name = data["name"]
    job_id = db.incr('id')
    body = {
        "id": job_id,
        "name": name,
        "created": get_current_time(),
        "status": "RUNNING",
        "last_updated": get_current_time(),
    }
    json_string = json.dumps(body)
    db.set(job_id, json_string)
    return create_response(data=body, response="Job created!")

@app.route("/pause", methods=['POST'])
def pause_job():
    data = request.form.to_dict()
    request_error = validate_id_request(data)
    if (request_error != None):
        return request_error
    job_id = data["id"]
    job = json.loads(db.get(job_id))
    status = job["status"]
    if(status != "RUNNING"):
        return create_response(success=False, error_message=f"Only RUNNING jobs can be paused. This job is {status}.")
    job["status"] = "PAUSED"
    job["last_updated"] = get_current_time()
    json_string = json.dumps(job)
    db.set(job_id, json_string)
    return create_response(data=job, response=f"Successfully paused job {job_id}")

@app.route("/resume", methods=['POST'])
def resume_job():
    data = request.form.to_dict()
    request_error = validate_id_request(data)
    if (request_error != None):
        return request_error
    job_id = data["id"]
    job = json.loads(db.get(job_id))
    status = job["status"]
    if(status != "PAUSED"):
        return create_response(success=False, error_message=f"Only PAUSED jobs can be resumed. This job is {status}.")
    job["status"] = "RUNNING"
    job["last_updated"] = get_current_time()
    json_string = json.dumps(job)
    db.set(job_id, json_string)
    return create_response(data=job, response=f"Successfully resumed job {job_id}")

@app.route("/stop", methods=['POST'])
def stop_job():
    data = request.form.to_dict()
    request_error = validate_id_request(data)
    if (request_error != None):
        return request_error
    job_id = data["id"]
    job = json.loads(db.get(job_id))
    status = job["status"]
    if(status != "RUNNING" and status != "PAUSED"):
        return create_response(success=False, error_message=f"Only RUNNING and PAUSED jobs can be stopped. This job is {status}.")
    job["status"] = "STOPPED"
    job["last_updated"] = get_current_time()
    json_string = json.dumps(job)
    db.set(job_id, json_string)
    return create_response(data=job, response=f"Successfully stopped job {job_id}")

@app.route("/jobs", methods=["GET"])
def get_all_jobs_route():
    return create_response(data = get_all_jobs(), response="All jobs in the database.")

@app.route("/jobs/<filter_type>/<filter>", methods=["GET"])
def get_filtered_jobs(filter_type, filter):
    if(filter_type == "id"):
        job_id = filter
        request_error = validate_id_request({"id": job_id})
        if (request_error != None):
            return request_error
        job = json.loads(db.get(job_id))
        return create_response(data=job, response=f"Requested job ID: {job_id}")
    elif(filter_type == "status"):
        status = filter
        if(status not in ["RUNNING", "PAUSED", "STOPPED"]):
            return create_response(success=False, error_message=f"Invalid status provided. Only RUNNING, PAUSED, STOPPED are valid statuses.")
        jobs = get_jobs_by_status(status)
        return create_response(data=jobs, response=f"Jobs with status {status}")
    else:
        return create_response(success=False, error_message="Invalid filter type provided")

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')

