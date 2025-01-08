from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from uuid import uuid4
import json
from cassandra.cluster import Cluster
import yaml

# Load Cassandra configuration from config.yaml
with open('config.yaml', 'r') as file:
    config_data = yaml.safe_load(file)
    cassandra_host = config_data['cassandra_host']
    keyspace = config_data['keyspace']

cluster = Cluster([cassandra_host])
session = cluster.connect(keyspace)

class CRUDRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed_path = urlparse(self.path)
        record_id = parsed_path.path.split('/')[-1]

        if record_id:
            record = self.read_record(record_id)
            if record:
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'id': str(record.id), 'name': record.name, 'age': record.age}).encode())
            else:
                self.send_response(404)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'message': 'Record not found'}).encode())
        else:
            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'message': 'Invalid request'}).encode())

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        parsed_data = json.loads(post_data.decode())

        name = parsed_data.get('name')
        age = parsed_data.get('age')

        if name and age:
            record_id = str(uuid4())
            self.create_record(record_id, name, age)

            self.send_response(201)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'id': record_id, 'message': 'Record created successfully'}).encode())
        else:
            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'message': 'Name and age are required'}).encode())

    def do_PUT(self):
        parsed_path = urlparse(self.path)
        record_id = parsed_path.path.split('/')[-1]

        if record_id:
            content_length = int(self.headers['Content-Length'])
            put_data = self.rfile.read(content_length)
            parsed_data = json.loads(put_data.decode())

            new_name = parsed_data.get('name')
            new_age = parsed_data.get('age')

            if new_name and new_age:
                self.update_record(record_id, new_name, new_age)

                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'id': record_id, 'message': 'Record updated successfully'}).encode())
            else:
                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'message': 'Name and age are required'}).encode())
        else:
            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'message': 'Invalid request'}).encode())

    def do_DELETE(self):
        parsed_path = urlparse(self.path)
        record_id = parsed_path.path.split('/')[-1]

        if record_id:
            self.delete_record(record_id)

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'id': record_id, 'message': 'Record deleted successfully'}).encode())
        else:
            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'message': 'Invalid request'}).encode())

    def create_record(self, record_id, name, age):
        query = f"INSERT INTO mytable (id, name, age) VALUES ({record_id}, '{name}', {age})"
        session.execute(query)

    def read_record(self, record_id):
        query = f"SELECT * FROM mytable WHERE id = {record_id}"
        result = session.execute(query)
        record = result.one()
        return record

    def update_record(self, record_id, new_name, new_age):
        query = f"UPDATE mytable SET name = '{new_name}', age = {new_age} WHERE id = {record_id}"
        session.execute(query)

    def delete_record(self, record_id):
        query = f"DELETE FROM mytable WHERE id = {record_id}"
        session.execute(query)

def run(server_class=HTTPServer, handler_class=CRUDRequestHandler, port=8080):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f'Starting HTTP server on port {port}...')
    httpd.serve_forever()

if __name__ == '__main__':
    run()
