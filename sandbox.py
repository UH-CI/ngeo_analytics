from threading import Lock, Event as Event_t, Condition
from multiprocessing import Process, Pipe, Event as Event_p
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait
import sys
import time



t_max = 5


def test():
    print("test")
    # sys.stdout.flush()
    # while True:
    #     #print(q)
    #     i = q.get(True, None)
    #     print(i)
    #     if i is None:
    #         q.put(None, True, None)
    #         q.close()
    #         break
        


def main():
    
    # q = multiprocessing.Manager().Queue()

    

    # print(q)
    # i = q.get(False, None)
    # print(i)

    t_executor = ProcessPoolExecutor(t_max)
    # for t in range(t_max):
    #     t_executor.submit(test, q, "test")

    # time.sleep(2)
    for i in range(50000):
        t_executor.submit(test)
    #     q.put(i, True, None)
    # q.put(None, True, None)

    #time.sleep(100)
    t_executor.shutdown(True)




if __name__ == "__main__":
    main()