import socket


def get_kvserver_response(server, port, request):
    s = socket.create_connection((server, port))
    s.sendall(request)
    response = s.recv(4096)
    str_response = response.decode('utf-8')
    return str_response
