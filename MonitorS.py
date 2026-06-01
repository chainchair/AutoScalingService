from concurrent import futures
import time
import grpc
import monitor_pb2
import monitor_pb2_grpc
import metrics_pb2
import metrics_pb2_grpc
import threading

class MonitorServiceServicer(monitor_pb2_grpc.MonitorServiceServicer):
    def __init__(self):
        self.metrics_store = {}
        self.registered_instances = {}
        self.lock = threading.Lock()

    def metrics_collector_loop(self):
        print("\n[MonitorS] Loop de recolección de métricas iniciado...")
        while True:
            time.sleep(5)  # (5 segundos)
            with self.lock:
                instances = list(self.registered_instances.items())
            
            for instance_id, ip in instances:
                try:
                    # Conectar al monitorC 
                    channel = grpc.insecure_channel(f'{ip}:50052')
                    stub = metrics_pb2_grpc.MonitorServiceStub(channel)
                    metrics = stub.GetNodeMetrics(metrics_pb2.Empty(), timeout=2)
                    
                    with self.lock:
                        self.metrics_store[instance_id] = {
                            'status': metrics.status,
                            'cpu_percent': metrics.cpu_percent,
                            'ram_percent': metrics.ram_percent,
                            'load_percent': metrics.load_percent,
                            'effective_load_percent': metrics.effective_load_percent,
                            'active_requests': metrics.active_requests,
                            'timestamp': metrics.timestamp
                        }
                    print(f"[MonitorS] Métricas recolectadas de {instance_id}: CPU {metrics.cpu_percent}% | RAM {metrics.ram_percent}%")
                except Exception as e:
                    print(f"[MonitorS] No se pudo recolectar métricas de {instance_id} ({ip}): {e}")

    def RegisterInstance(self, request, context):

        print(
            f"REGISTER -> "
            f"{request.instance_id} "
            f"{request.ip_address}"
        )

        with self.lock:
            self.registered_instances[
                request.instance_id
            ] = request.ip_address

        print(
            f"TOTAL REGISTERED = "
            f"{len(self.registered_instances)}"
        )

        return monitor_pb2.RegisterResponse(
            success=True,
            message="Instancia registrada con éxito"
        )

    def DeregisterInstance(self, request, context):
        print(f"Desregistrando instancia: {request.instance_id}")
        with self.lock:
            if request.instance_id in self.registered_instances:
                del self.registered_instances[request.instance_id]
            if request.instance_id in self.metrics_store:
                del self.metrics_store[request.instance_id]
        return monitor_pb2.RegisterResponse(success=True, message="Instancia desregistrada con éxito")

    def SendMetrics(self, request, context):
        print(
            f"Métricas recibidas de {request.instance_id}: "
            f"status={request.status}, "
            f"CPU={request.cpu_percent}%, "
            f"RAM={request.ram_percent}%, "
            f"load={request.load_percent}%, "
            f"effective_load={request.effective_load_percent}%, "
            f"active_requests={request.active_requests}"
        )
        with self.lock:
            self.metrics_store[request.instance_id] = {
                'status': request.status,
                'cpu_percent': request.cpu_percent,
                'ram_percent': request.ram_percent,
                'load_percent': request.load_percent,
                'effective_load_percent': request.effective_load_percent,
                'active_requests': request.active_requests,
                'timestamp': request.timestamp
            }
        return monitor_pb2.MetricsResponse(success=True)

    def GetMetrics(self, request, context):
        response = monitor_pb2.MetricsList()
        with self.lock:
            for instance_id, metrics in self.metrics_store.items():
                instance_metrics = monitor_pb2.InstanceMetrics(
                    instance_id=instance_id,
                    status=metrics['status'],
                    cpu_percent=metrics['cpu_percent'],
                    ram_percent=metrics['ram_percent'],
                    load_percent=metrics['load_percent'],
                    effective_load_percent=metrics['effective_load_percent'],
                    active_requests=metrics['active_requests'],
                    timestamp=metrics['timestamp']
                )
                response.instances.append(instance_metrics)
        return response

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = MonitorServiceServicer()
    monitor_pb2_grpc.add_MonitorServiceServicer_to_server(servicer, server)
    
    # Iniciar el hilo recolector de métricas en segundo plano
    threading.Thread(target=servicer.metrics_collector_loop, daemon=True).start()
    
    server.add_insecure_port('[::]:50051')
    print("MonitorS (Servidor) iniciando en el puerto 50051...")
    server.start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
