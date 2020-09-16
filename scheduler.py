#!/usr/bin/env python

import scheduler
import config
import multiprocessing

if __name__ == '__main__':
    multiprocessing.set_start_method("spawn")
    processes = []
    for i in range(config.THREADS):
        proc = multiprocessing.Process(target=scheduler.run_scheduler, args=())
        processes.append(proc)
        proc.start()
    try:
        while True:
            pass
    except KeyboardInterrupt:
        for proc in processes:
            proc.kill()
