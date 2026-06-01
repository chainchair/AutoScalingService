import grpc
import time
import json
import boto3

import monitor_pb2
import monitor_pb2_grpc

# Controlador ASG muy básico que se conecta al MonitorService para obtener métricas y tomar decisiones de escalado.
class ControllerASG:

    def __init__(self, config):

        self.config = config

        self.channel = grpc.insecure_channel(
            f"{config['monitor_host']}:{config['monitor_port']}"
        )

        self.stub = monitor_pb2_grpc.MonitorServiceStub(
            self.channel
        )

        self.last_scaling_action = 0

        self.high_load_count = 0
        self.low_load_count = 0

        self.ec2 = boto3.client(
            "ec2",
            region_name="us-east-1"
        )

    # Método para obtener métricas de todas las instancias registradas en el MonitorService.
    def get_metrics(self):

        response = self.stub.GetMetrics(
            monitor_pb2.Empty()
        )

        return response.instances

    # Métodos para calcular promedios de CPU y RAM a partir de las métricas recolectadas.
    def calculate_average_cpu(self, instances):

        if not instances:
            return 0

        return sum(
            i.cpu_percent
            for i in instances
        ) / len(instances)

    # Método para calcular el promedio de RAM utilizado por las instancias. Se puede usar para políticas de escalado basadas en RAM.
    def calculate_average_ram(self, instances):

        if not instances:
            return 0

        return sum(
            i.ram_percent
            for i in instances
        ) / len(instances)

    # Método para evaluar la política de escalado basada en el promedio de CPU. 
    # Retorna "scale_up", "scale_down" o "keep" según corresponda.
    def evaluate_policy(self, instances):

        cpu_avg = self.calculate_average_cpu(instances)

        current_count = len(instances)

        print("\n========== CONTROLLER ==========")
        print(f"Instances: {current_count}")
        print(f"CPU Average: {cpu_avg:.2f}%")
        print(f"High Counter: {self.high_load_count}")
        print(f"Low Counter: {self.low_load_count}")
        print("================================")

        if cpu_avg > self.config["scale_up_cpu"]:

            self.high_load_count += 1
            self.low_load_count = 0

            print(
                f"[Controller] High load count: "
                f"{self.high_load_count}/"
                f"{self.config['high_load_threshold_count']}"
            )

            if (
                self.high_load_count >=
                self.config["high_load_threshold_count"]
            ):
                self.high_load_count = 0
                return "scale_up"

        elif cpu_avg < self.config["scale_down_cpu"]:

            self.low_load_count += 1
            self.high_load_count = 0

            print(
                f"[Controller] Low load count: "
                f"{self.low_load_count}/"
                f"{self.config['low_load_threshold_count']}"
            )

            if (
                self.low_load_count >=
                self.config["low_load_threshold_count"]
            ):
                self.low_load_count = 0
                return "scale_down"

        else:

            self.high_load_count = 0
            self.low_load_count = 0

        return "keep"

    # Método para verificar si el período de cooldown está activo, lo que impide acciones de escalado demasiado frecuentes.
    def cooldown_active(self):

        return (
            time.time() - self.last_scaling_action
        ) < self.config["cooldown_seconds"]

    # Métodos de escalado que simulan la creación y terminación de instancias. 
    # En una implementación real, aquí se usarían llamadas a la API de AWS (boto3) para gestionar las instancias EC2.
    def scale_up(self):

        print("\n==============================")
        print("[Controller] DECISION: SCALE UP")
        print("[Controller] Creating instance")
        print("==============================\n")

        self.last_scaling_action = time.time()

        self.create_instance()

    # Método para escalar hacia abajo, que simula la terminación de una instancia.
    # En una implementación real, se elegiría una instancia específica para terminar.
    def scale_down(self):

        print("\n==============================")
        print("[Controller] DECISION: SCALE DOWN")
        print("[Controller] Removing instance")
        print("==============================\n")

        self.last_scaling_action = time.time()

        # Aquí después llamarás terminate_instance()

    # Método simulado para crear una instancia EC2. 
    # En una implementación real, usarías boto3 para lanzar una nueva instancia con la AMI y configuración deseada.
    def create_instance(self):

        try:

            response = self.ec2.run_instances(
                ImageId=self.config["ami_id"],
                InstanceType=self.config["instance_type"],
                KeyName=self.config["key_name"],
                SecurityGroupIds=[
                    self.config["security_group_id"]
                ],
                SubnetId=self.config["subnet_id"],
                MinCount=1,
                MaxCount=1
            )

            instance_id = (
                response["Instances"][0]["InstanceId"]
            )

            print(
                f"[AWS] Instance created: "
                f"{instance_id}"
            )

        except Exception as e:

            print(
                f"[AWS] Error creating instance: "
                f"{e}"
            )

    # Método simulado para terminar una instancia EC2.
    # En una implementación real, usarías boto3 para detener la instancia específica.
    def terminate_instance(self, instance_id):

        try:

            self.ec2.terminate_instances(
                InstanceIds=[instance_id]
            )

            print(
                f"[AWS] Instance terminated: "
                f"{instance_id}"
            )

        except Exception as e:

            print(
                f"[AWS] Error terminating instance: "
                f"{e}"
            )

    # Método principal del controlador que ejecuta el loop de monitoreo y toma decisiones de escalado basadas en las métricas recolectadas.
    def run(self):

        while True:

            try:

                instances = self.get_metrics()

                count = len(instances)

                # Garantizar mínimo de instancias
                if count < self.config["min_instances"]:

                    print(
                        "[Controller] Current instances "
                        f"({count}) below minimum "
                        f"({self.config['min_instances']})"
                    )

                    if not self.cooldown_active():
                        self.scale_up()

                    time.sleep(10)
                    continue

                action = self.evaluate_policy(
                    instances
                )

                if self.cooldown_active():

                    print(
                        "[Controller] Cooldown active"
                    )

                else:

                    if (
                        action == "scale_up"
                        and count < self.config["max_instances"]
                    ):
                        self.scale_up()

                    elif (
                        action == "scale_down"
                        and count > self.config["min_instances"]
                    ):
                        self.scale_down()

                    elif (
                        action == "scale_up"
                        and count >= self.config["max_instances"]
                    ):
                        print(
                            "[Controller] Scale up blocked: "
                            "maximum instances reached"
                        )

                    elif (
                        action == "scale_down"
                        and count <= self.config["min_instances"]
                    ):
                        print(
                            "[Controller] Scale down blocked: "
                            "minimum instances reached"
                        )

                    else:

                        print(
                            "[Controller] Decision: KEEP"
                        )

                time.sleep(10)

            except Exception as e:

                print(
                    f"[Controller] Error: {e}"
                )

                time.sleep(10)

# Punto de entrada del programa.
if __name__ == "__main__":

    with open("asg_config.json") as f:
        config = json.load(f)

    controller = ControllerASG(config)

    controller.create_instance()