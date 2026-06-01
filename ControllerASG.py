import grpc
import time
import json
import boto3

import monitor_pb2
import monitor_pb2_grpc

class ControllerASG:

    def __init__(self, config):
        self.config = config
        self.ec2 = boto3.client("ec2", region_name="us-east-1")
        
        # USAR IP FIJA - la que ya funciona
        monitors_ip = "172.31.36.19"
        print(f"[Controller] Usando MonitorS IP fija: {monitors_ip}")
        
        self.channel = grpc.insecure_channel(f"{monitors_ip}:{config['monitor_port']}")
        self.stub = monitor_pb2_grpc.MonitorServiceStub(self.channel)
        
        self.last_scaling_action = 0
        self.high_load_count = 0
        self.low_load_count = 0

    def get_running_instances_count(self):
        """Obtiene el número REAL de instancias EC2 en ejecución"""
        try:
            response = self.ec2.describe_instances(
                Filters=[
                    {'Name': 'instance-state-name', 'Values': ['running']}
                ]
            )
            
            # Excluir la instancia de MonitorS (la que tiene IP 172.31.36.19)
            count = 0
            for reservation in response['Reservations']:
                for instance in reservation['Instances']:
                    private_ip = instance.get('PrivateIpAddress', '')
                    # No contar la instancia de MonitorS
                    if private_ip != "172.31.36.19":
                        count += 1
            
            print(f"[DEBUG] EC2 running instances (excluyendo MonitorS): {count}")
            return count
        except Exception as e:
            print(f"[ERROR] Error getting instance count: {e}")
            return 0

    def get_oldest_instances(self, count):
        """Obtiene las instancias más antiguas para terminar"""
        try:
            response = self.ec2.describe_instances(
                Filters=[
                    {'Name': 'instance-state-name', 'Values': ['running']}
                ]
            )
            
            instances = []
            for reservation in response['Reservations']:
                for instance in reservation['Instances']:
                    private_ip = instance.get('PrivateIpAddress', '')
                    # No terminar la instancia de MonitorS
                    if private_ip != "172.31.36.19":
                        instances.append({
                            'id': instance['InstanceId'],
                            'launch_time': instance['LaunchTime']
                        })
            
            instances.sort(key=lambda x: x['launch_time'])
            return [inst['id'] for inst in instances[:count]]
        except Exception as e:
            print(f"[ERROR] Error getting oldest instances: {e}")
            return []

    def enforce_min_max_policy(self):
        """Fuerza las políticas mínimo/máximo"""
        current_count = self.get_running_instances_count()
        
        print(f"[POLICY] Instancias actuales: {current_count}, Min: {self.config['min_instances']}, Max: {self.config['max_instances']}")
        
        if current_count < self.config["min_instances"]:
            print(f"[POLICY] Escalando UP para alcanzar mínimo")
            needed = self.config["min_instances"] - current_count
            
            for i in range(needed):
                if current_count + i < self.config["max_instances"]:
                    print(f"[POLICY] Creando instancia ({i+1}/{needed})")
                    self.create_instance()
                    time.sleep(15)
        
        elif current_count > self.config["max_instances"]:
            print(f"[POLICY] Escalando DOWN para respetar máximo")
            excess = current_count - self.config["max_instances"]
            
            instances_to_terminate = self.get_oldest_instances(excess)
            for instance_id in instances_to_terminate:
                print(f"[POLICY] Terminando instancia: {instance_id}")
                self.terminate_instance(instance_id)
                time.sleep(10)

    def get_metrics(self):
        try:
            response = self.stub.GetMetrics(monitor_pb2.Empty())
            return response.instances
        except Exception as e:
            print(f"[ERROR] Error getting metrics: {e}")
            return []

    def calculate_average_cpu(self, instances):
        if not instances:
            return 0
        return sum(i.cpu_percent for i in instances) / len(instances)

    def evaluate_policy(self, instances):
        cpu_avg = self.calculate_average_cpu(instances)
        current_count = len(instances)

        print(f"\n========== CONTROLLER ==========")
        print(f"Instancias registradas: {current_count}")
        print(f"CPU Promedio: {cpu_avg:.2f}%")
        print(f"High Counter: {self.high_load_count}")
        print(f"Low Counter: {self.low_load_count}")
        print("================================")

        if cpu_avg > self.config["scale_up_cpu"]:
            self.high_load_count += 1
            self.low_load_count = 0
            print(f"[Controller] High load: {self.high_load_count}/{self.config['high_load_threshold_count']}")
            
            if self.high_load_count >= self.config["high_load_threshold_count"]:
                self.high_load_count = 0
                return "scale_up"

        elif cpu_avg < self.config["scale_down_cpu"]:
            self.low_load_count += 1
            self.high_load_count = 0
            print(f"[Controller] Low load: {self.low_load_count}/{self.config['low_load_threshold_count']}")
            
            if self.low_load_count >= self.config["low_load_threshold_count"]:
                self.low_load_count = 0
                return "scale_down"
        else:
            self.high_load_count = 0
            self.low_load_count = 0

        return "keep"

    def cooldown_active(self):
        return (time.time() - self.last_scaling_action) < self.config["cooldown_seconds"]

    def create_instance(self):
        try:
            print("[AWS] Creando nueva instancia...")
            
            response = self.ec2.run_instances(
                ImageId=self.config["ami_id"],
                InstanceType=self.config["instance_type"],
                KeyName=self.config["key_name"],
                SecurityGroupIds=[self.config["security_group_id"]],
                SubnetId=self.config["subnet_id"],
                MinCount=1,
                MaxCount=1
            )
            
            instance_id = response["Instances"][0]["InstanceId"]
            print(f"[AWS] ✅ Instancia creada: {instance_id}")
            return instance_id
            
        except Exception as e:
            print(f"[AWS] ❌ Error creando instancia: {e}")
            return None

    def terminate_instance(self, instance_id):
        try:
            self.ec2.terminate_instances(InstanceIds=[instance_id])
            print(f"[AWS] Instancia terminada: {instance_id}")
        except Exception as e:
            print(f"[AWS] Error terminando instancia: {e}")

    def scale_up(self):
        current_instances = self.get_running_instances_count()
        
        if current_instances >= self.config["max_instances"]:
            print("[Controller] Máximo alcanzado, no se puede escalar UP")
            return
        
        print("\n==============================")
        print("[Controller] DECISIÓN: ESCALAR UP")
        print("==============================\n")
        
        self.last_scaling_action = time.time()
        self.create_instance()

    def scale_down(self):
        print("\n==============================")
        print("[Controller] DECISIÓN: ESCALAR DOWN")
        print("==============================\n")
        
        self.last_scaling_action = time.time()
        
        oldest = self.get_oldest_instances(1)
        if oldest:
            self.terminate_instance(oldest[0])

    def run(self):
        print("[Controller] Iniciando loop principal...")
        
        while True:
            try:
                # 1. Primero forzar políticas min/max
                self.enforce_min_max_policy()
                
                # 2. Obtener métricas
                instances = self.get_metrics()
                count = self.get_running_instances_count()
                
                print(f"\n[STATUS] Instancias EC2: {count} | Registradas en MonitorS: {len(instances)}")
                
                # 3. Verificar cooldown
                if self.cooldown_active():
                    print(f"[Controller] Cooldown activo ({self.config['cooldown_seconds']}s)")
                    time.sleep(10)
                    continue
                
                # 4. Evaluar política de escalado
                action = self.evaluate_policy(instances)
                
                # 5. Ejecutar acción
                if action == "scale_up" and count < self.config["max_instances"]:
                    self.scale_up()
                elif action == "scale_down" and count > self.config["min_instances"]:
                    self.scale_down()
                else:
                    print(f"[Controller] Sin acción (Acción: {action}, Count: {count})")
                
                time.sleep(10)
                
            except Exception as e:
                print(f"[Controller] Error en loop: {e}")
                time.sleep(10)

if __name__ == "__main__":
    with open("asg_config.json") as f:
        config = json.load(f)
    
    print("[Controller] Configuración cargada:")
    print(f"  - Min instances: {config['min_instances']}")
    print(f"  - Max instances: {config['max_instances']}")
    print(f"  - Scale up CPU: {config['scale_up_cpu']}%")
    print(f"  - Scale down CPU: {config['scale_down_cpu']}%")
    
    controller = ControllerASG(config)
    controller.run()