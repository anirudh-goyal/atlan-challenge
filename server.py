from flask import Flask
import redis
import time

app = Flask(__name__)
db = redis.Redis(host='redis', port=6379)

@app.route("/")
def home():
    retries = 5
    while True:
        try:
            return str(db.incr('hits')) + "\n"
        except redis.exceptions.ConnectionError as exc:
            if retries == 0:
                raise exc
            retries -= 1
            time.sleep(0.5)
    
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')

