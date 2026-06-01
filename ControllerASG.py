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
        
        # Obtener IP real de MonitorS
        monitors_ip = self.get_monitors_ip()
        print(f"[Controller] MonitorS IP: {monitors_ip}")
        
        self.channel = grpc.insecure_channel(f"{monitors_ip}:{config['monitor_port']}")
        self.stub = monitor_pb2_grpc.MonitorServiceStub(self.channel)
        
        self.last_scaling_action = 0
        self.high_load_count = 0
        self.low_load_count = 0

    def get_monitors_ip(self):
        """Obtiene la IP de MonitorS usando tags de AWS"""
        try:
            response = self.ec2.describe_instances(
                Filters=[
                    {'Name': 'tag:Role', 'Values': ['MonitorS']},
                    {'Name': 'instance-state-name', 'Values': ['running']}
                ]
            )
            
            for reservation in response['Reservations']:
                for instance in reservation['Instances']:
                    ip = instance['PrivateIpAddress']
                    print(f"[Controller] Found MonitorS at {ip}")
                    return ip
            
            print(f"[Controller] MonitorS not found, using config: {self.config['monitor_host']}")
            return self.config['monitor_host']
        except Exception as e:
            print(f"[Controller] Error finding MonitorS: {e}")
            return self.config['monitor_host']

    def get_running_instances_count(self):
        """Obtiene el número REAL de instancias EC2 en ejecución"""
        try:
            response = self.ec2.describe_instances(
                Filters=[
                    {'Name': 'tag:Environment', 'Values': ['ASG']},
                    {'Name': 'instance-state-name', 'Values': ['running']}
                ]
            )
            
            count = 0
            for reservation in response['Reservations']:
                count += len(reservation['Instances'])
            
            return count
        except Exception as e:
            print(f"[Controller] Error getting instance count: {e}")
            return len(self.get_metrics())

    def get_oldest_instances(self, count):
        """Obtiene las instancias más antiguas para terminar"""
        try:
            response = self.ec2.describe_instances(
                Filters=[
                    {'Name': 'tag:Environment', 'Values': ['ASG']},
                    {'Name': 'instance-state-name', 'Values': ['running']}
                ]
            )
            
            instances = []
            for reservation in response['Reservations']:
                for instance in reservation['Instances']:
                    instances.append({
                        'id': instance['InstanceId'],
                        'launch_time': instance['LaunchTime']
                    })
            
            instances.sort(key=lambda x: x['launch_time'])
            return [inst['id'] for inst in instances[:count]]
        except Exception as e:
            print(f"[Controller] Error getting oldest instances: {e}")
            return []

    def enforce_min_max_policy(self):
        """Fuerza las políticas mínimo/máximo"""
        current_count = self.get_running_instances_count()
        
        if current_count < self.config["min_instances"]:
            print(f"[POLICY] Instances ({current_count}) < MIN ({self.config['min_instances']})")
            needed = self.config["min_instances"] - current_count
            
            for i in range(needed):
                if current_count + i < self.config["max_instances"]:
                    print(f"[POLICY] Creating instance ({i+1}/{needed})")
                    self.create_instance()
                    time.sleep(15)
        
        elif current_count > self.config["max_instances"]:
            print(f"[POLICY] Instances ({current_count}) > MAX ({self.config['max_instances']})")
            excess = current_count - self.config["max_instances"]
            
            instances_to_terminate = self.get_oldest_instances(excess)
            for instance_id in instances_to_terminate:
                print(f"[POLICY] Terminating instance: {instance_id}")
                self.terminate_instance(instance_id)
                time.sleep(10)

    def get_metrics(self):
        try:
            response = self.stub.GetMetrics(monitor_pb2.Empty())
            return response.instances
        except Exception as e:
            print(f"[Controller] Error getting metrics: {e}")
            return []

    def calculate_average_cpu(self, instances):
        if not instances:
            return 0
        return sum(i.cpu_percent for i in instances) / len(instances)

    def calculate_average_ram(self, instances):
        if not instances:
            return 0
        return sum(i.ram_percent for i in instances) / len(instances)

    def evaluate_policy(self, instances):
        cpu_avg = self.calculate_average_cpu(instances)
        current_count = len(instances)

        print(f"\n========== CONTROLLER ==========")
        print(f"Instances: {current_count}")
        print(f"CPU Average: {cpu_avg:.2f}%")
        print(f"High Counter: {self.high_load_count}")
        print(f"Low Counter: {self.low_load_count}")
        print("================================")

        if cpu_avg > self.config["scale_up_cpu"]:
            self.high_load_count += 1
            self.low_load_count = 0
            print(f"[Controller] High load count: {self.high_load_count}/{self.config['high_load_threshold_count']}")
            
            if self.high_load_count >= self.config["high_load_threshold_count"]:
                self.high_load_count = 0
                return "scale_up"

        elif cpu_avg < self.config["scale_down_cpu"]:
            self.low_load_count += 1
            self.high_load_count = 0
            print(f"[Controller] Low load count: {self.low_load_count}/{self.config['low_load_threshold_count']}")
            
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
            monitors_ip = self.get_monitors_ip()
            
            user_data_script = f"""#!/bin/bash
cat > /home/ubuntu/monitor_config.sh << EOF
export MONITORS_IP={monitors_ip}
EOF

# Actualizar monitorC.py con la IP correcta
sed -i 's/MONITORS_IP = ".*"/MONITORS_IP = "{monitors_ip}"/' /home/ubuntu/monitorC.py 2>/dev/null || true

# Iniciar servicios
cd /home/ubuntu
python3 monitorC.py &
"""
            
            response = self.ec2.run_instances(
                ImageId=self.config["ami_id"],
                InstanceType=self.config["instance_type"],
                KeyName=self.config["key_name"],
                SecurityGroupIds=[self.config["security_group_id"]],
                SubnetId=self.config["subnet_id"],
                MinCount=1,
                MaxCount=1,
                UserData=user_data_script,
                TagSpecifications=[{
                    'ResourceType': 'instance',
                    'Tags': [
                        {'Key': 'Environment', 'Value': 'ASG'},
                        {'Key': 'Role', 'Value': 'AppInstance'},
                        {'Key': 'ManagedBy', 'Value': 'ControllerASG'}
                    ]
                }]
            )
            
            instance_id = response["Instances"][0]["InstanceId"]
            print(f"[AWS] Instance created: {instance_id}")
            return instance_id
            
        except Exception as e:
            print(f"[AWS] Error creating instance: {e}")
            return None

    def terminate_instance(self, instance_id):
        try:
            self.ec2.terminate_instances(InstanceIds=[instance_id])
            print(f"[AWS] Instance terminated: {instance_id}")
        except Exception as e:
            print(f"[AWS] Error terminating instance: {e}")

    def scale_up(self):
        current_instances = self.get_running_instances_count()
        
        if current_instances >= self.config["max_instances"]:
            print("[Controller] Maximum instances reached")
            return
        
        print("\n==============================")
        print("[Controller] DECISION: SCALE UP")
        print("==============================\n")
        
        self.last_scaling_action = time.time()
        self.create_instance()

    def scale_down(self):
        print("\n==============================")
        print("[Controller] DECISION: SCALE DOWN")
        print("==============================\n")
        
        self.last_scaling_action = time.time()
        
        # Terminar la instancia más antigua
        oldest = self.get_oldest_instances(1)
        if oldest:
            self.terminate_instance(oldest[0])

    def run(self):
        while True:
            try:
                self.enforce_min_max_policy()
                
                instances = self.get_metrics()
                count = self.get_running_instances_count()
                
                print(f"\n[STATUS] Running: {count} | Metrics: {len(instances)}")
                
                if self.cooldown_active():
                    print("[Controller] Cooldown active")
                    time.sleep(10)
                    continue
                
                action = self.evaluate_policy(instances)
                
                if action == "scale_up" and count < self.config["max_instances"]:
                    self.scale_up()
                elif action == "scale_down" and count > self.config["min_instances"]:
                    self.scale_down()
                else:
                    print(f"[Controller] No action (Action: {action}, Count: {count})")
                
                time.sleep(10)
                
            except Exception as e:
                print(f"[Controller] Error: {e}")
                time.sleep(10)

if __name__ == "__main__":
    with open("asg_config.json") as f:
        config = json.load(f)
    
    controller = ControllerASG(config)
    controller.run()