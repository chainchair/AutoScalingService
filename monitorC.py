from concurrent import futures
import requests
import sys
import signal
import time
import os

import grpc
import metrics_pb2 
import metrics_pb2_grpc 
import monitor_pb2
import monitor_pb2_grpc

# Constants for the Flask app endpoints
# Estas URLs asumen que monitorC y la app Flask corren en la MISMA instancia
FLASK_METRICS_URL = "http://127.0.0.1:5000/metrics"
FLASK_HEALTH_URL = "http://127.0.0.1:5000/health"

# Configuraciones de red (usan variables de entorno, o '127.0.0.1' por defecto para pruebas locales)
MONITORS_IP = "172.31.36.19"      # IP privada de MonitorS
MY_IP = requests.get(
    "http://169.254.169.254/latest/meta-data/local-ipv4"
).text
           # IP privada de esta AppInstance
INSTANCE_ID = requests.get(
    "http://169.254.169.254/latest/meta-data/instance-id"
).text


class MonitorService(metrics_pb2_grpc.MonitorServiceServicer):

    def Ping(self, request, context):
        try:
            response = requests.get(FLASK_HEALTH_URL,timeout=2)

            if response.status_code != 200:
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                context.set_details("App healthcheck failed")
                return metrics_pb2.Pong() # type: ignore
            data = response.json()

            if data.get("status") != "alive":
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                context.set_details("App not alive")
                return metrics_pb2.Pong() # type: ignore
            return metrics_pb2.Pong(message="pong")# type: ignore

        except Exception as e:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(f"AppInstance unreachable: {str(e)}")
            return metrics_pb2.Pong()# type: ignore

    def GetNodeMetrics(self, request, context):
        try:
            response = requests.get(FLASK_METRICS_URL,timeout=2)

            if response.status_code != 200:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("Metrics endpoint failed")
                return metrics_pb2.InstanceMetrics()# type: ignore
            data = response.json()

            return metrics_pb2.InstanceMetrics(# type: ignore
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
            context.set_details(str(e))
            return metrics_pb2.InstanceMetrics()# type: ignore

def handle_shutdown(signum, frame):
    print("\nRecibida señal de apagado. Desregistrando instancia...")
    try:
        channel = grpc.insecure_channel(f'{MONITORS_IP}:50051')
        stub = monitor_pb2_grpc.MonitorServiceStub(channel)
        request = monitor_pb2.RegisterRequest(
            instance_id=INSTANCE_ID,
            ip_address=MY_IP
        )
        response = stub.DeregisterInstance(request, timeout=5)
        print(f"Éxito: {response.message}")
    except Exception as e:
        print(f"Advertencia: No se pudo desregistrar en MonitorS: {e}")
    print("Saliendo...")
    sys.exit(0)

def serve():
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    metrics_pb2_grpc.add_MonitorServiceServicer_to_server(
        MonitorService(),
        server
    )

    server.add_insecure_port("[::]:50052")
    server.start()
    print("MonitorC gRPC server running on port 50052")

    max_retries = 5
    for attempt in range(max_retries):
        try:
            print(f"Intentando registrar instancia en MonitorS ({MONITORS_IP}) (intento {attempt + 1}/{max_retries})...")
            channel = grpc.insecure_channel(f'{MONITORS_IP}:50051')
            stub = monitor_pb2_grpc.MonitorServiceStub(channel)
            request = monitor_pb2.RegisterRequest(
                instance_id=INSTANCE_ID,
                ip_address=MY_IP
            )
            response = stub.RegisterInstance(request, timeout=5)
            print(f"Éxito: {response.message}")
            break
        except Exception as e:
            print(f"Advertencia: No se pudo registrar en MonitorS (intento {attempt + 1})")
            if attempt < max_retries - 1:
                time.sleep(3)

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        handle_shutdown(signal.SIGINT, None)

if __name__ == "__main__":
    serve()