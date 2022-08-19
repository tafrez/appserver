import base64
import io
from threading import Thread
from time import sleep

from PIL import Image
from azure.servicebus import ServiceBusClient, ServiceBusReceiveMode, ServiceBusMessage
from flask import Flask, Response

app = Flask(__name__)

stop_run = True
app_queue = "appqueue"
web_queue = "webqueue"
connection_str = "Endpoint=sb://lab204servicebus038.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=lJQdozyLphVTxCvSOLA7VftKYODOXRFndob8GgnnqY8="


def process_msgs():
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=connection_str, logging_enable=True)
    with servicebus_client:
        # get the Queue Receiver object for the queue
        receiver = servicebus_client.get_queue_receiver(queue_name=app_queue, max_wait_time=5,
                                                        receive_mode=ServiceBusReceiveMode.PEEK_LOCK)
        msgs = receiver.receive_messages(max_message_count=1)
        try:
            if len(msgs) == 1:
                msg = msgs[0]
                print(str(msg))
                print(f"Received: {str(msg.body) }.")
                img_str = base64.b64decode(str(msg))
                thumb_str = create_thumbnail(img_str)
                publish_thumbnail(thumb_str)
                receiver.complete_message(message=msg)
        except Exception as e:
            receiver.complete_message(message=msg)
            print(e)


def create_thumbnail(img_str):
    buf = io.BytesIO(img_str)
    img = Image.open(buf)
    img.thumbnail((120, 120))
    buffered = io.BytesIO()
    img.save(buffered, format="JPEG")
    thumb_str = base64.b64encode(buffered.getvalue()).decode()
    return thumb_str


def publish_thumbnail(thumb_str):
    message = ServiceBusMessage(thumb_str.encode("utf-8"))
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=connection_str, logging_enable=True)
    with servicebus_client:
        # get a Queue Sender object to send messages to the queue
        sender = servicebus_client.get_queue_sender(queue_name=web_queue)
        with sender:
            sender.send_messages(message)
            print(f"Message published to topic: {web_queue}")


def my_function():
    global stop_run
    while not stop_run:
        process_msgs()
        sleep(5)
        print("running...")
    else:
        print("stopped..")


def manual_run():
    t = Thread(target=my_function)
    t.start()
    return "Processing"


@app.route("/stop", methods=['GET'])
def stop_process():
    global stop_run
    if stop_run:
        return "Application is already in Stopped"
    else:
        stop_run = True
        return "Application stopped"


@app.route("/run", methods=['GET'])
def run_process():
    global stop_run
    if stop_run:
        stop_run = False
        return Response(manual_run(), mimetype="text/html")
    else:
        return "Application is already in Running"


@app.route("/", methods=['GET'])
def status():
    global stop_run
    if stop_run:
        return "Application Stopped"
    else:
        return "Application Running"


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80, debug=True)
