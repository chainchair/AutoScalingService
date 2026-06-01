# tag_monitors.py - Ejecuta este script UNA SOLA VEZ en tu máquina local
import boto3

ec2 = boto3.client('ec2', region_name='us-east-1')

# CAMBIA ESTE ID por el ID de tu instancia MonitorS (el que tiene 172.31.36.19)
MONITORS_INSTANCE_ID = "i-0522492b3d77c7b6d"  # <--- CAMBIA ESTO

try:
    ec2.create_tags(
        Resources=[MONITORS_INSTANCE_ID],
        Tags=[
            {'Key': 'Role', 'Value': 'MonitorS'},
            {'Key': 'Environment', 'Value': 'ASG'}
        ]
    )
    print(f"Tagged MonitorS instance {MONITORS_INSTANCE_ID}")
    
    # Verificar
    response = ec2.describe_instances(InstanceIds=[MONITORS_INSTANCE_ID])
    tags = response['Reservations'][0]['Instances'][0].get('Tags', [])
    print(f"Tags actuales: {tags}")
    
except Exception as e:
    print(f"❌ Error: {e}")