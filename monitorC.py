from concurrent import futures
import requests
import sys
import signal
import time
import os
import boto3

import grpc
import metrics_pb2 
import metrics_pb2_grpc 
import monitor_pb2
import monitor_pb2_grpc

FLASK_METRICS_URL = "http://127.0.0.1:5000/metrics"
FLASK_HEALTH_URL = "http://127.0.0.1:5000/health"

def find_monitors_ip():
    """Busca la instancia de MonitorS automáticamente"""
    try:
        ec2 = boto3.client('ec2', region_name='us-east-1')
        response = ec2.describe_instances(
            Filters=[
                {'Name': 'tag:Role', 'Values': ['MonitorS']},
                {'Name': 'instance-state-name', 'Values': ['running']}
            ]
        )
        
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                ip = instance['PrivateIpAddress']
                print(f"[MonitorC] Found MonitorS at {ip}")
                return ip
        
        return os.environ.get('MONITORS_IP', '172.31.36.19')
    except Exception as e:
        print(f"[MonitorC] Error finding MonitorS: {e}")
        return os.environ.get('MONITORS_IP', '172.31.36.19')

# Configuración dinámica
MONITORS_IP = find_monitors_ip()
MY_IP = requests.get("http://169.254.169.254/latest/meta-data/local-ipv4").text
INSTANCE_ID = requests.get("http://169.254.169.254/latest/meta-data/instance-id").text

print(f"[MonitorC] My IP: {MY_IP}, My ID: {INSTANCE_ID}")
print(f"[MonitorC] MonitorS IP: {MONITORS_IP}")

class MonitorService(metrics_pb2_grpc.MonitorServiceServicer):

    def Ping(self, request, context):
        try:
            response = requests.get(FLASK_HEALTH_URL, timeout=2)
            if response.status_code != 200:
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                return metrics_pb2.Pong()
            
            data = response.json()
            if data.get("status") != "alive":
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                return metrics_pb2.Pong()
            
            return metrics_pb2.Pong(message="pong")
        except Exception as e:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return metrics_pb2.Pong()

    def GetNodeMetrics(self, request, context):
        try:
            response = requests.get(FLASK_METRICS_URL, timeout=2)
            if response.status_code != 200:
                context.set_code(grpc.StatusCode.INTERNAL)
                return metrics_pb2.InstanceMetrics()
            
            data = response.json()
            return metrics_pb2.InstanceMetrics(
                instance_id=INSTANCE_ID,
                status=data["status"],
                cpu_percent=data["cpu_percent"],
                ram_percent=data["ram_percent"],
                load_percent=data["load_percent"],
                effective_load_percent=data["effective_load_percent"],
                active_requests=data["active_requests"],
                timestamp=data["timestamp"]
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            return metrics_pb2.InstanceMetrics()

def handle_shutdown(signum, frame):
    print("\n[MonitorC] Shutting down, deregistering...")
    try:
        channel = grpc.insecure_channel(f'{MONITORS_IP}:50051')
        stub = monitor_pb2_grpc.MonitorServiceStub(channel)
        request = monitor_pb2.RegisterRequest(instance_id=INSTANCE_ID, ip_address=MY_IP)
        response = stub.DeregisterInstance(request, timeout=5)
        print(f"[MonitorC] {response.message}")
    except Exception as e:
        print(f"[MonitorC] Could not deregister: {e}")
    sys.exit(0)

def serve():
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    metrics_pb2_grpc.add_MonitorServiceServicer_to_server(MonitorService(), server)
    server.add_insecure_port("[::]:50052")
    server.start()
    print("[MonitorC] gRPC server running on port 50052")

    # Registrar con MonitorS (con reintentos)
    max_retries = 10
    for attempt in range(max_retries):
        try:
            print(f"[MonitorC] Registering with MonitorS ({MONITORS_IP}) attempt {attempt+1}/{max_retries}...")
            channel = grpc.insecure_channel(f'{MONITORS_IP}:50051')
            stub = monitor_pb2_grpc.MonitorServiceStub(channel)
            request = monitor_pb2.RegisterRequest(instance_id=INSTANCE_ID, ip_address=MY_IP)
            response = stub.RegisterInstance(request, timeout=5)
            print(f"[MonitorC] SUCCESS: {response.message}")
            break
        except Exception as e:
            print(f"[MonitorC] Registration failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(5)

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        handle_shutdown(signal.SIGINT, None)

if __name__ == "__main__":
    serve()