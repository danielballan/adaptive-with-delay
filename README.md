# Adaptive with Delay

An adaptive scan where the data of interest comes from a flyer and is delayed by
a (simulated) message bus.

Since this demo uses a
[PR branch of bluesky](https://github.com/bluesky/bluesky/pull/1256) it may be
best to first create a separate environment for it.

```
python -m venv adaptive-with-delay
source adaptive-with-delay/bin/activate
```

Install requirements:

```
pip install -r requirements.txt
```

Start IOC:

```
python ioc.py
```

Run example:

```
ipython -i example.py
RE(plan(0.5))
```
