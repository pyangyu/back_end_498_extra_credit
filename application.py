from flask import Flask, jsonify, request
import os
import pymysql

application = Flask(__name__)

#Endpoint: Health Check
@application.route('/health', methods=['GET'])
def health():
    """
    This endpoint is used by the autograder to confirm that the backend deployment is healthy.
    """
    return jsonify({"status": "healthy"}), 200

#Endpoint: Data Insertion
@application.route('/events', methods=['POST'])
def create_event():
    """
    This endpoint should eventually insert data into the database.
    The database communication is currently stubbed out.
    You must implement insert_data_into_db() function to integrate with your MySQL RDS Instance.
    """
    try:
        payload = request.get_json()
        required_fields = ["title", "date"]
        if not payload or not all(field in payload for field in required_fields):
            return jsonify({"error": "Missing required fields: 'title' and 'date'"}), 400

        insert_data_into_db(payload)
        return jsonify({"message": "Event created successfully"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500

#Endpoint: Data Retrieval
@application.route('/data', methods=['GET'])
def get_data():
    """
    This endpoint should eventually provide data from the database.
    The database communication is currently stubbed out.
    You must implement the fetch_data_from_db() function to integrate with your MySQL RDS Instance.
    """
    try:
        data = fetch_data_from_db()
        return jsonify({"data": data}), 200
    except NotImplementedError:
        return jsonify({"error": "Database functionality not implemented."}), 501

def get_db_connection():
    """
    Establish and return a connection to the RDS MySQL database.
    The following variables should be added to the Elastic Beanstalk Environment Properties for better security. Follow guidelines for more info.
      - DB_HOST
      - DB_USER
      - DB_PASSWORD
      - DB_NAME
    """
    DB_HOST = os.environ.get("DB_HOST")
    DB_USER = os.environ.get("DB_USER")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    DB_NAME = os.environ.get("DB_NAME")
    connection = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        db=DB_NAME
    )
    return connection

def create_db_table():
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS events (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                description TEXT,
                date DATE NOT NULL,
                location VARCHAR(255)
            )
            """
            cursor.execute(create_table_sql)
        connection.commit()
        print('Events table created or already exists')
    finally:
        connection.close()

def insert_data_into_db(payload):
    """
    Stub for database communication.
    Implement this function to insert the data into the database.
    NOTE: Our autograder will automatically insert data into the DB automatically keeping in mind the explained SCHEMA, you dont have to insert your own data.
    """
    create_db_table()
    # TODO: Implement the database call    
    
    raise NotImplementedError("Database insert function not implemented.")

#Database Function Stub
def fetch_data_from_db():
    """
    Stub for database communication.
    Implement this function to fetch your data from the database.
    """
    # TODO: Implement the database call
    
    raise NotImplementedError("Database fetch function not implemented.")

if __name__ == '__main__':
    application.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))

