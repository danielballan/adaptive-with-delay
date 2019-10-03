import threading
import time
import random
import queue
import bluesky.plan_stubs as bps
from ophyd import Device, Component, EpicsSignal
from ophyd.sim import NullStatus


class Detector(Device):
    x = Component(EpicsSignal, ':x')

    def kickoff(self):
        return NullStatus()

    def complete(self):
        return NullStatus()

    def collect(self):
        ...


det = Detector('random_walk', name='det')


def fake_kafka(q, poison):
    from caproto.sync.client import subscribe
    sub = subscribe('random_walk:x')
    
    def put_into_bus(reading):
        time.sleep(random.random())
        value = reading.data[0]
        print(f'putting {value} into message bus')
        if poison:
            sub.interrupt()
        q.put(value)

    sub.add_callback(put_into_bus)
    sub.block()

def plan(threshold):
    q = queue.Queue()
    poison = []
    thread = threading.Thread(target=fake_kafka, args=(q, poison))
    thread.start()

    yield from bps.open_run()
    yield from bps.kickoff(det, wait=True)
    latest_news = -1000000
    while True:
        # Drain the message bus.
        while True:
            try:
                latest_news = q.get_nowait()
                print(f'received {latest_news} from message bus')
            except queue.Empty:
                break
        if latest_news > threshold:
            break
    poison.append(object())
    thread.join()
    yield from bps.close_run()



from bluesky import RunEngine
RE = RunEngine()
