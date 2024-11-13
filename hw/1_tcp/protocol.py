import socket
import numpy as np
import threading
import time
import struct
import os

class UDPBasedProtocol:
    def __init__(self, *, local_addr, remote_addr):
        self.udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.remote_addr = remote_addr
        self.udp_socket.bind(local_addr)

    def sendto(self, data):
        return self.udp_socket.sendto(data, self.remote_addr)

    def recvfrom(self, n):
        msg, addr = self.udp_socket.recvfrom(n)
        return msg

    def close(self):
        self.udp_socket.close()


# class MyTCPProtocol(UDPBasedProtocol):
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)

#     def send(self, data: bytes):
#         return self.sendto(data)

#     def recv(self, n: int):
#         return self.recvfrom(n)
    
#     def close(self):
#         super().close()

no = 98
def unpack_meta(meta):
    segment_begin, size_and_request = struct.unpack('@ih', meta)
    size = size_and_request & 1023
    request = (size_and_request & 1024) > 0
    return segment_begin, segment_begin + size, request

class MyTCPProtocol(UDPBasedProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.udp_socket.settimeout(0.00001)
        self.segment_size = 0
        self.send_buffer = np.ndarray(10000000, dtype = 'byte')
        self.send_start = 0
        self.recv_start = 0
        self.recv_buffer = np.ndarray(10000000, dtype = 'byte')
        self.valid = np.zeros(10000000, dtype = 'bool')
        self.padding = np.zeros(1000, dtype = 'byte').tobytes()

        self.running = True
        self.eventEnd = threading.Event()
        # self.recv_buffer_lock = threading.Lock()
        # self.send_buffer_lock = threading.Lock()
        self.listener = threading.Thread(target=self._listener_work)
        self.initialized = False
        global no
        self.id = no
        no = 99

        self.sendtry = 0
        self.recvtry = 0
        self.waitForSegmentBegin = None
        self.waitForSegmentEnd = None
        self.waitForSegmentEvent = threading.Event()
        self.waitForSegmentEvent.set()


    def _listener_work(self):
        highier_send_begin = -1
        while self.running:
            if self.eventEnd.is_set():
                return
            try:
                # print('start listen', self.id)
                segment = self.recvfrom(self.segment_size + 6)
            except:
                if not self.waitForSegmentEvent.is_set():
                    self._send_segment(self.waitForSegmentBegin, self.waitForSegmentEnd, True)
                    # print(self.id, "send request", self.recvtry, self.waitForSegmentBegin, self.waitForSegmentEnd)
            else:
                segment_begin, segment_end, is_request = unpack_meta(segment[:6])
                # print(self.id, "recv", self.recvtry, segment_begin, segment_end, is_request)

                if not is_request:
                        if not self.valid[segment_begin]:
                            data_array = np.frombuffer(segment[6 : segment_end - segment_begin + 6], dtype=self.recv_buffer.dtype)
                            # print(self.id, "recv",  self.recvtry, data_array)
                            self.recv_buffer[segment_begin : segment_end] = data_array
                            self.valid[segment_begin] = True # ?

                        if not self.waitForSegmentEvent.is_set() and segment_begin == self.waitForSegmentBegin:
                            self.waitForSegmentEvent.set()
                else:
                    # if segment_begin == segment_end:
                    #     self._send_segment(segment_begin + 1, segment_begin + 1, True)
                    #     self.segment_size = segment_begin

                    if segment_begin < highier_send_begin:
                        continue
                    # with self.send_buffer_lock:
                    if self.send_start >= segment_end:
                        self._send_segment(segment_begin, segment_end, False)
                        # print(self.id, "send data", self.recvtry, segment_begin, segment_end)
                    highier_send_begin = segment_begin
                # self.recvtry += 1
                


    def _send_segment(self, begin, end, is_request):
        # print(self.id, "send", self.sendtry ,begin, end, 1 if is_request else 0, file=sys.stderr)
        size_and_request = end - begin
        size_and_request += 1024 if is_request else 0
        segment = struct.pack('@ih', begin, size_and_request)   # meta info
        if not is_request:
            # print(self.id, "send", self.sendtry, self.send_buffer[begin : end])
            data = self.send_buffer[begin : end].tobytes()
            segment += data
        # self.sendtry += 1
        segment += self.padding[:self.segment_size + 6 - len(segment)]
        
        self.sendto(segment)
        

    def _send_data(self, begin, end):
        data_size = end - begin
        num_segments = data_size / self.segment_size
        if int(num_segments) != num_segments:
            num_segments = int(num_segments) + 1
        num_segments = int(num_segments)

        for i in range(num_segments):
            segment_begin = i * self.segment_size 
            segment_end = min((i + 1) * self.segment_size, data_size)
            self._send_segment(segment_begin, segment_end, False)
    
    def send(self, data: bytes):
        # print(self.id, "send",  file=sys.stderr)
        data_array = np.frombuffer(data, dtype=self.send_buffer.dtype)
        if not self.initialized:
            self.segment_size = min(data_array.shape[0], 1000)
            self.listener.start()
            self.initialized = True
            # self._send_segment(data_array.shape[0], data_array.shape[0], True)

        begin = self.send_start
        end = self.send_start + data_array.shape[0]

        # with self.send_buffer_lock:
        self.send_buffer[begin : end] = data_array
        self.send_start += data_array.shape[0]

        self._send_data(begin, end)

        return data_array.shape[0]

    def _validate_segment(self, begin, end):
        
        if self.waitForSegmentEvent.is_set() and not self.valid[begin]:
            self.waitForSegmentBegin = begin
            self.waitForSegmentEnd = end
            self.waitForSegmentEvent.clear()
            self.waitForSegmentEvent.wait()

    def _validate_data(self, data_begin, data_end):
        data_size = (data_end - data_begin)
        num_segments = data_size / self.segment_size
        if int(num_segments) != num_segments:
            num_segments = int(num_segments) + 1
        num_segments = int(num_segments)

        for i in range(num_segments):
            segment_begin = data_begin + i * self.segment_size 
            segment_end   = data_begin + min((i + 1) * self.segment_size, data_size)
            self._validate_segment(segment_begin, segment_end)
    
    def recv(self, n: int):
        if not self.initialized:
            self.segment_size = min(n, 1000)
            self.listener.start()
            self.initialized = True
           
        self._validate_data(self.recv_start, self.recv_start + n)
        # with self.recv_buffer_lock:
        data = self.recv_buffer[self.recv_start : self.recv_start + n].tobytes()
        self.recv_start += n
        return data
    
    def close(self):
        self.running = False
        self.eventEnd.set()
        self.listener.join()
        super().close()

