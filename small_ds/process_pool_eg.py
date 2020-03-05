from concurrent.futures import ProcessPoolExecutor
import time
from datetime import datetime

def current_time():
    return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def wait_function(x, y,i):
    print("task {}  function {}".format(i, current_time()))
    print("\n")
    time.sleep(2)
    #print('Task(', x, 'multiply', y, ') completed')
    return x * y,i


def callback_function(future):
    print('Callback with the following result', future.result())


def main():
    with ProcessPoolExecutor(max_workers=2) as executor:
        for i in range(1,10):
            future = executor.submit(wait_function, 3*i, 4*i, i)
            future.add_done_callback(callback_function)


if __name__ == '__main__':
    main()