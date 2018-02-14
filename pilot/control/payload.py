#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2016-2017
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Tobias Wegner, tobias.wegner@cern.ch, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017
# - Wen Guan, wen.guan@cern.ch, 2017-2018

import Queue
import json
import os
import threading
import time
from collections import defaultdict

from pilot.control.payloads import generic, eventservice
from pilot.control.job import send_state
from pilot.util.config import config

import logging

# import pprint
logger = logging.getLogger(__name__)


def control(queues, traces, args):
    """
    (add description)

    :param queues:
    :param traces:
    :param args:
    :return:
    """

    threads = [threading.Thread(target=validate_pre,
                                kwargs={'queues': queues,
                                        'traces': traces,
                                        'args': args}),
               threading.Thread(target=execute_payloads,
                                kwargs={'queues': queues,
                                        'traces': traces,
                                        'args': args}),
               threading.Thread(target=validate_post,
                                kwargs={'queues': queues,
                                        'traces': traces,
                                        'args': args}),
               threading.Thread(target=failed_post,
                                kwargs={'queues': queues,
                                        'traces': traces,
                                        'args': args})]
    [t.start() for t in threads]


def validate_pre(queues, traces, args):
    """
    (add description)

    :param queues:
    :param traces:
    :param args:
    :return:
    """
    while not args.graceful_stop.is_set():
        try:
            job = queues.payloads.get(block=True, timeout=1)
        except Queue.Empty:
            continue

        if _validate_payload(job):
            queues.validated_payloads.put(job)
        else:
            queues.failed_payloads.put(job)


def _validate_payload(job):
    """
    (add description)

    :param job:
    :return:
    """
    # valid = random.uniform(0, 100)
    # if valid > 99:
    #     logger.warning('payload did not validate correctly -- skipping')
    #     job['errno'] = random.randint(0, 100)
    #     job['errmsg'] = 'payload failed random validation'
    #     return False
    return True


def set_time_consumed(t_tuple):
    """
    Set the system+user time spent by the payload.
    The cpuConsumptionTime is the system+user time while wall time is encoded in pilotTiming (third number).
    Previously the cpuConsumptionTime was "corrected" with a scaling factor but this was deemed outdated and is now set
    to 1.
    The t_tuple is defined as map(lambda x, y:x-y, t1, t0), here t0 and t1 are os.times() measured before and after
    the payload execution command.

    :param t_tuple: map(lambda x, y:x-y, t1, t0)
    :return: cpu_consumption_unit, cpu_consumption_time, cpu_conversion_factor
    """

    t_tot = reduce(lambda x, y:x+y, t_tuple[2:3])
    cpu_conversion_factor = 1.0
    cpu_consumption_unit = "s" # "kSI2kseconds"
    cpu_consumption_time = int(t_tot * cpu_conversion_factor)

    return cpu_consumption_unit, cpu_consumption_time, cpu_conversion_factor


def execute_payloads(queues, traces, args):
    """
    Execute queued payloads.

    :param queues:
    :param traces:
    :param args:
    :return:
    """
    while not args.graceful_stop.is_set():
        try:
            job = queues.validated_payloads.get(block=True, timeout=1)
            log = logger.getChild(str(job['PandaID']))

            q_snapshot = list(queues.finished_data_in.queue)
            peek = [s_job for s_job in q_snapshot if job['PandaID'] == s_job['PandaID']]
            if len(peek) == 0:
                queues.validated_payloads.put(job)
                for i in xrange(10):
                    if args.graceful_stop.is_set():
                        break
                    time.sleep(0.1)
                continue

            out = open(os.path.join(job['working_dir'], 'payload.stdout'), 'wb')
            err = open(os.path.join(job['working_dir'], 'payload.stderr'), 'wb')

            send_state(job, args, 'starting')

            if job.get('eventService', '').lower() == "true":
                payload_executor = eventservice.Executor(args, job, out, err)
            else:
                payload_executor = generic.Executor(args, job, out, err)

            # run the payload and measure the execution time
            t0 = os.times()
            exit_code = payload_executor.run()
            t1 = os.times()
            t = map(lambda x, y: x - y, t1, t0)
            job['cpuConsumptionUnit'], job['cpuConsumptionTime'], job['cpuConversionFactor'] = set_time_consumed(t)

            out.close()
            err.close()

            if exit_code == 0:
                job['transExitCode'] = 0
                queues.finished_payloads.put(job)
                job['state'] = 'finished'
            else:
                job['transExitCode'] = exit_code
                queues.failed_payloads.put(job)
                job['state'] = 'failed'

        except Queue.Empty:
            continue


def process_job_report(job):
    """
    Process the required job report.
    Payload error codes and diagnostics, as well as payload metadata (for output files) and stageout type will be
    extracted. The stageout type is either "all" (i.e. stage-out both output and log files) or "log" (i.e. only log file
    will be staged out).
    Note: some fields might be experiment specific. A call to a user function is therefore also done.

    :param job: job dictionary will be updated by the function and several fields set.
    :return:
    """

    log.info('processing job report')
    with open(os.path.join(job['working_dir'], config.Payload.jobreport)) as data_file:
        # compulsory field; the payload must procude a job report (see config file for file name)
        job['metaData'] = json.load(data_file)

        # extract user specific info from job report
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], -1)
        user.update_job_data(job)


def validate_post(queues, traces, args):
    """
    Validate finished payloads.
    If payload finished correctly, add the job to the data_out queue. If it failed, add it to the data_out queue as
    well but only for log stage-out.

    :param queues:
    :param traces:
    :param args:
    :return:
    """

    while not args.graceful_stop.is_set():
        # finished payloads
        try:
            job = queues.finished_payloads.get(block=True, timeout=1)
        except Queue.Empty:
            continue
        log = logger.getChild(str(job['PandaID']))

        # note: all PanDA users should generate a job report json file (required by Harvester)
        # process the job report and set multiple fields
        process_job_report(job)

        log.debug('adding job to data_out queue')
        queues.data_out.put(job)


def failed_post(queues, traces, args):
    """
    (add description)

    :param queues:
    :param traces:
    :param args:
    :return:
    """

    while not args.graceful_stop.is_set():
        # finished payloads
        try:
            job = queues.failed_payloads.get(block=True, timeout=1)
        except Queue.Empty:
            continue
        log = logger.getChild(str(job['PandaID']))

        log.debug('adding log for log stageout')

        job['stageout'] = "log"  # only stage-out log file
        queues.data_out.put(job)
