import itertools
import threading
import time
import random
import queue
import bluesky.plan_stubs as bps
import numpy
from ophyd import Device, Component, EpicsSignal
from ophyd.sim import NullStatus
from caproto.sync.client import subscribe, write, read
from caproto.threading.client import Context

class Detector(Device):

    def kickoff(self):
        # Drain the queue.
        while True:
            try:
                _ = receive_queue.get_nowait()
            except queue.Empty:
                break
        self.counter = itertools.count()
        return NullStatus()

    def complete(self):
        return NullStatus()

    def describe_collect(self):
        return {'primary':
            {'x': {'dtype': 'number', 'source': 'my PV', 'shape': []},
             'pos': {'dtype': 'number', 'source': 'another PV', 'shape': []}}}

    def read_configuration(self):
        return OrderedDict()

    def describe_configuration(self):
        return OrderedDict()

    def collect(self):
        # Drain the message bus.
        while True:
            try:
                det_reading, pos_reading = receive_queue.get_nowait()
                # print(f'received {det_reading.data[0]} from message bus')
            except queue.Empty:
                raise StopIteration
            i = next(self.counter)
            yield {'data': {'x': det_reading.data[0],
                            'pos': pos_reading.data[0]},
                   'timestamps': {'x': det_reading.metadata.timestamp,
                                  'pos': pos_reading.metadata.timestamp},
                   'time': i,
                   'seq_num': i}


det = Detector('random_walk', name='det')
pos = EpicsSignal('random_walk:pos', name='pos')
write('random_walk:dt', 0.1)


def fake_kafka(q):
    sub = subscribe('random_walk:x', data_type='time')
    
    def put_into_bus(det_reading):
        if not pos_readings:
            # print('no pos readings')
            return
        pos_reading = pos_readings[-1]
        # print(f'x={det_reading.data[0]:.3} pos={pos_reading.data[0]:.3}')
        q.put((det_reading, pos_reading))


    sub.add_callback(put_into_bus)
    sub.block()


# Run fake kafka.
send_queue = queue.Queue()
thread = threading.Thread(target=fake_kafka, args=(send_queue,))
thread.start()

def simulated_traffic(send_queue, receive_queue):
    while True:
        item = send_queue.get()
        time.sleep(random.random())
        receive_queue.put(item)

receive_queue = queue.Queue()
traffic_thread = threading.Thread(
    target=simulated_traffic, args=(send_queue, receive_queue))
traffic_thread.start()

# Subscribe to position
ctx = Context()
pv, = ctx.get_pvs('random_walk:pos')
pos_subscription = pv.subscribe(data_type='time')
pos_readings = []

def append_pos_reading(reading):
    pos_readings.append(reading)

pos_subscription.add_callback(append_pos_reading)


def plan(threshold):
    yield from bps.open_run()
    yield from bps.kickoff(det, wait=True)
    target_pos = -3.0
    while True:
        yield from bps.mv(pos, target_pos)
        yield from bps.sleep(0.1)  # fake motor delay
        payload = yield from bps.collect(det, stream=True)
        for reading in payload:
            x = reading['data']['x']
            historical_pos = reading['data']['pos']
            print(f"current={target_pos:.3} historical={historical_pos:.3} x={x:.3}")
            if x > threshold:
                yield from bps.close_run()
                print("DONE!")
                return
        target_pos += 0.1

# beamline setup code

from bluesky import RunEngine
from bluesky.callbacks.mpl_plotting import LivePlot
RE = RunEngine()
