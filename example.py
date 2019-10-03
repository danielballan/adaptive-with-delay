import itertools
import threading
import time
import random
import queue
import bluesky.plan_stubs as bps
import numpy
from ophyd import Device, Component, EpicsSignal
from ophyd.sim import NullStatus
from caproto.sync.client import subscribe, write

class Detector(Device):
    x = Component(EpicsSignal, ':x')

    def kickoff(self):
        # Drain the queue.
        while True:
            try:
                reading = q.get_nowait()
            except queue.Empty:
                break
        self.counter = itertools.count()
        return NullStatus()

    def complete(self):
        return NullStatus()

    def describe_collect(self):
        return {'primary':
            {'x': {'dtype': 'number', 'source': self.x._read_pv.pvname, 'shape': []}}}

    def read_configuration(self):
        return OrderedDict()

    def describe_configuration(self):
        return OrderedDict()

    def collect(self):
        # Drain the message bus.
        while True:
            try:
                reading = q.get_nowait()
                print(f'received {reading.data[0]} from message bus')
            except queue.Empty:
                raise StopIteration
            i = next(self.counter)
            yield {'data': {'x': reading.data[0]},
                   'timestamps': {'x': reading.metadata.timestamp},
                   'time': i,
                   'seq_num': i}


det = Detector('random_walk', name='det')
pos = EpicsSignal('random_walk:pos', name='pos')
write('random_walk:dt', 0.1)


def fake_kafka(q, event):
    sub = subscribe('random_walk:x', data_type='time')
    
    def put_into_bus(reading):
        time.sleep(random.random())
        if event.is_set():
            # print(f'putting {reading.data} into message bus')
            q.put(reading)

    sub.add_callback(put_into_bus)
    sub.block()


# Run fake kafka.
q = queue.Queue()
event = threading.Event()
thread = threading.Thread(target=fake_kafka, args=(q, event))
thread.start()
event.set()


def plan(threshold):
    yield from bps.open_run()
    yield from bps.kickoff(det, wait=True)
    for i in numpy.linspace(-3, 3, 200):
        print(f'motor is at {i:.3}')
        yield from bps.mv(pos, i)
        yield from bps.sleep(0.1)  # fake motor delay
        payload = yield from bps.collect(det)
        for reading in payload:
            value = reading['data']['x']
            print(f'value {value}')
            if value > threshold:
                yield from bps.close_run()
                return


from bluesky import RunEngine
RE = RunEngine()
