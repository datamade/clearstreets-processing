import logging
from scipy.optimize import fsolve
import numpy
import time
from collections import deque

def poll(iterable, key=None, min_pause=10) :
    if key is None :
        key = lambda x : x

    intervals = deque([60], 3)
    changed = deque([True], 3)

    item = next(iterable)
    yield item

    last_key = key(item)

    t0 = time.clock()
    time.sleep(1)

    while True :
        item = next(iterable)

        intervals.append(time.clock() - t0)
        t0 = time.clock()

        if last_key != key(item) :
            yield item
            changed.append(True)
            last_key = key(item)
        else :
            changed.append(False)

        estimated_pause = bestPause(intervals,
                                    changed)

        pause = max(min_pause, estimated_pause)

        logging.info("Sampling Interval: {} seconds".format(pause))

        time.sleep(pause)



def bestPause(intervals, changed) :

    # Rate of change estimator for irregularly sampled data
    # we will want to solve for x
    #
    # Cho and Garcia Molina, 2003, Estimating Frequency of Change
    # http://dl.acm.org/citation.cfm?id=857170
    intervals = numpy.array(intervals)
    changed = numpy.array(changed)

    changed_intervals = intervals[changed]
    unchanged_time = numpy.sum(intervals[changed == False])

    icgm = lambda x : (numpy.sum(changed_intervals/
                                 (numpy.exp(x * changed_intervals) - 1))
                       - unchanged_time)


    estimated_rate = fsolve(icgm, .01)[0]

    logging.info("Estimated Average Update Interval: {} seconds".format(1.0/estimated_rate))


    # Assuming that updates are drawn from a Poisson distribution,
    # then with some probability, we will observe LESS than 2 events
    # in this period. This does not mean the probability that we will
    # observe 1 update, as it is very likely that we will observe no
    # update.
    #
    # We also do not allow the interval to get too small so we don't
    # slam the city's servers.
    #
    # .95 : .355362
    # .90 : .531812
    # .80 : .824388
    #
    # http://www.wolframalpha.com/input/?i=exp(-bx)%2Bbxexp(-bx)%3D.8

    estimated_pause = .35262/estimated_rate

    return estimated_pause
