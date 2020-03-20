import serial # pip install pyserial
import json
import time
import threading
from six.moves import input
from struct import pack, unpack
from azure.iot.device import MethodResponse, Message, IoTHubDeviceClient # pip install azure-iothub-device-client

#设置端口 & 波特率
port_name = "COM2"
baud_rate = 9600

#连接RTU
tty_client = serial.Serial(
port=port_name,                 # number of device, numbering starts at
                                # zero. if everything fails, the user
                                # can specify a device string, note
                                # that this isn't portable anymore
                                # if no port is specified an unconfigured
                                # an closed serial port object is created
baudrate=baud_rate,             # baud rate
bytesize=serial.EIGHTBITS,      # number of databits
parity=serial.PARITY_NONE,      # enable parity checking
stopbits=serial.STOPBITS_ONE,   # number of stopbits
timeout=0,                      # set a timeout value, None for waiting forever
xonxoff=0,                      # enable software flow control
rtscts=0,                       # enable RTS/CTS flow control
interCharTimeout=None           # Inter-character timeout, None to disable
)

#连接Azure IoT Hub
CONNECTION_STRING = "{your connection string here}"

device_client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)

device_client.connect()

#定义C2D Message监听器
def message_listener(device_client):
    while True:
        message = device_client.receive_message()  # blocking call
        decoded_message = message.data.decode()
        print(type(decoded_message))
        print("Writing message to RTU: ", decoded_message)
        #判断数据类型进行写入操作，全部以C2D Message标准String格式写入
        if isinstance(decoded_message, dict):
            tty_client.write(json.dumps(decoded_message).encode())
        elif isinstance(decoded_message, (int, float)):
            tty_client.write(decoded_message)
        else:
            tty_client.write(decoded_message.encode())
        #print("custom properties are")
        #print(message.custom_properties)

#定义C2D Method监听器，在Method_Name为”Write_Value“时将数据写入RTU，返回成功200
def method_listener(device_client):
    while True:
        method_request = device_client.receive_method_request("Write_Value")  # Wait for method1 calls
        decoded_method = method_request.payload
        print(type(decoded_method))
        print("Writing message to RTU:", decoded_method)
        #判断数据类型执行写入操作
        if isinstance(decoded_method, dict):
            tty_client.write(json.dumps(decoded_method).encode())
        elif isinstance(decoded_method, int):
            tty_client.write(decoded_method)
        elif isinstance(decoded_method,float):
            byte=pack('d',decoded_method)
            tty_client.write(byte)
        else:
            tty_client.write(decoded_method.encode())
        payload = {"result": True, "data": "Write to RTU Successfully"}  # set response payload
        status = 200  # set return status code
        method_response = MethodResponse.create_from_method_request(
            method_request, status, payload
        )
        device_client.send_method_response(method_response)  # send response     

#收到无法解析的C2D Method，返回错误400
def generic_method_listener(device_client):
    while True:
        method_request = device_client.receive_method_request()  # Wait for unknown method calls
        print("Method Name is: ", method_request.name)
        print("Method Payload is ",method_request.payload)
        payload = {"result": False, "data": "unknown method"}  # set response payload
        status = 400  # set return status code
        print("executed unknown method: " + method_request.name)
        method_response = MethodResponse.create_from_method_request(
            method_request, status, payload
        )
        device_client.send_method_response(method_response)  # send response     

#定义RTU监听器，读取RTU数据
def tty_listener(tty_client):
    if tty_client.isOpen() == True:
        print("Port Opened !")
        while True:
            time.sleep(1)
            data = (tty_client.read(size=10))
            if len(data) > 0:
                #print(type(data))
                print("Received Data From ", port_name, ": ", data.decode())
                #tty_client.write("ack".encode())
                message = {
                    "Device_ID": "001",
                    "Data_Name": "Voltage",
                    "Data": data.decode()
                }
                print("Sending Messgage to Azure ", message)
                device_client.send_message(json.dumps(message))

#定义个监听器线程并启动线程
message_listener_thread = threading.Thread(target=message_listener, args=(device_client,))
message_listener_thread.daemon = True
message_listener_thread.start()

method_listener_thread = threading.Thread(target=method_listener, args=(device_client,))
method_listener_thread.daemon = True
method_listener_thread.start()

generic_method_listener_thread = threading.Thread(target=generic_method_listener, args=(device_client,))
generic_method_listener_thread.daemon = True
generic_method_listener_thread.start()

tty_listener_thread = threading.Thread(target=tty_listener, args=(tty_client,))
tty_listener_thread.daemon = True
tty_listener_thread.start()

#当输入Q字符时程序退出
while True:
    selection = input("Press Q to quit\n")
    if selection == "Q" or selection == "q":
        print("Quitting...")
        break

#断开IoT Hub及关闭RTU端口
device_client.disconnect()
tty_client.close()