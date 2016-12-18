import time
import sys

def doTask():
    time.sleep(0.1)

def example_1(n):
    steps = n/10
    for i in range(n):
        doTask()
        if i%steps == 0:
            print('\b.', end="", flush=True)
    print(' Done!')

print('Starting ', end="", flush=True)
example_1(100)