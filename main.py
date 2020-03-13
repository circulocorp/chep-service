from classes.chep import Chep
from PydoNovosoft.utils import Utils
from PydoNovosoft.slack import Slack
import datetime
import os
import pika
import json_logging
import logging
import json
import requests
import sys


json_logging.ENABLE_JSON_LOGGING = True
json_logging.init()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))

config = Utils.read_config("package.json")
env_cfg = config[os.environ["environment"]]
url = env_cfg["API_URL"]
rabbitmq = env_cfg["RABBITMQ_URL"]

chep_url = "http://ws4.altotrack.com/WSPosiciones_Chep/WSPosiciones_Chep.svc?wsdl"

if env_cfg["secrets"]:
    rabbit_user = Utils.get_secret("rabbitmq_user")
    rabbit_pass = Utils.get_secret("rabbitmq_passw")
else:
    rabbit_user = env_cfg["rabbitmq_user"]
    rabbit_pass = env_cfg["rabbitmq_passw"]


chep = Chep("", "", chep_url)


def get_vehicle(unit_id):
    response = requests.get(url+"/api/vehicles?Unit_Id="+unit_id)
    vehicles = response.json()
    if len(vehicles) > 0:
        return vehicles[0]
    else:
        logger.error("Vehicle not found", extra={'props': {"vehicle": unit_id, "app": config["name"],
                                                           "label": config["name"]}})
        return None


def send(data):
    logger.info("Sending data to Altotrack",
                extra={'props': {"raw": data, "app": config["name"], "label": config["name"]}})
    resp = chep.send_events(data)
    if resp == 'Ok':
        print("Data was accepted")
        logger.info("Data was Accepted",
                    extra={'props': {"raw": data, "app": config["name"], "label": config["name"]}})
    else:
        print("Data was rejected")
        logger.error("Data was rejected",
                     extra={'props': {"raw": data, "app": config["name"], "label": config["name"]}})


def fix_data(msg):
    print("Reading events")
    root = {}
    registro = dict()
    movil = []
    data = json.loads(msg)
    goods = []
    bads = []
    for event in data["events"]:
        mov = dict()
        obj = event["header"]
        vehicle = get_vehicle(obj["UnitId"])
        if vehicle is not None and "provider" in vehicle:
            mov["proveedor"] = "TELCEL-GVT"
            mov["nombremovil"] = ""
            mov["patente"] = vehicle["Registration"]
            mov["ignicion"] = "1"
            mov["GPSLinea"] = "1"
            mov["LOGGPS"] = "0"
            mov["puerta1"] = "0"
            mov["evento"] = "1"
            mov["latitud"] = format(float(event["header"]["Latitude"]), '.6f')
            mov["longitud"] = format(float(event["header"]["Longitude"]), '.6f')
            mov["direccion"] = event["header"]["Direction"]
            mov["velocidad"] = event["header"]["Speed"]
            mov["fecha"] = Utils.format_date(Utils.datetime_zone(Utils.utc_to_datetime(
                event["header"]["UtcTimestampSeconds"]), "America/Mexico_City"), '%d-%m-%Y %H:%M:%S')
            if mov["latitud"] == 0 or mov["longitud"] == 0:
                logger.error("Wrong Lat and Long",
                               extra={'props': {"raw": mov, "app": config["name"], "label": config["name"]}})
                bad = dict()
                bad["vehicle"] = vehicle["_id"]
                bad["unit"] = obj["UnitId"]
                bad["problem"] = "Latitud="+str(mov["latitud"])+", Longitud="+str(mov["longitud"])
                bads.append(bad)
            else:
                movil.append(mov)
                good = dict()
                good["vehicle"] = vehicle["_id"]
                goods.append(good)
        else:
            bad = dict()
            bad["problem"] = "Vehiculo no registrado o sin proveedor"
            bad["unit"] = obj["UnitId"]
            logger.error("Vehiculo no registrado o sin proveedor",
                         extra={'props': {"raw": bad, "app": config["name"], "label": config["name"]}})
    registro["movil"] = movil
    root["registro"] = registro
    send(root)


def callback(ch, method, properties, body):
    logger.info("Reading message", extra={'props': {"raw": json.loads(body), "app": config["name"],
                                                    "label": config["name"]}})
    fix_data(body)


def start():
    credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)
    parameters = pika.ConnectionParameters(rabbitmq, 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(config["queue"], durable=True)
    channel.basic_consume(callback, config["queue"], no_ack=True)
    logger.info("Connection successful to RabbitMQ", extra={'props': {"app": config["name"], "label": config["name"]}})
    print("Connection successful to RabbitMQ")
    channel.start_consuming()


def main():
    print(Utils.print_title("package.json"))
    start()


if __name__ == '__main__':
    main()
