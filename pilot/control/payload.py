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

from pilot.control import payloads
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


def execute(queues, traces, args):
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

            log.debug('opening payload stdout/err logs')
            out = open(os.path.join(job['working_dir'], 'payload.stdout'), 'wb')
            err = open(os.path.join(job['working_dir'], 'payload.stderr'), 'wb')

            log.debug('setting up payload environment')
            send_state(job, args, 'starting')

            if job.get('eventService', '').lower() == "true":
                payload_executor = payloads.eventservice.Executor(args, job, out, err)
            else:
                payload_executor = payloads.generic.Executor(args, job, out, err)
            exit_code = payload_executor.run()

            log.debug('closing payload stdout/err logs')
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

            dump_job_report(job, 'job_report_dump.json')

        except Queue.Empty:
            continue


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
        log.debug('extracting job report')
        stageout = "all"
        with open(os.path.join(job['working_dir'], config.Payload.jobreport)) as data_file:
            job['job_report'] = json.load(data_file)

            # extract info from job report
            # === experiment specific ===
            if 'exeErrorCode' in job['job_report']:
                job['exeErrorCode'] = job['job_report']['exeErrorCode']
                if job['exeErrorCode'] == 0:
                    stageout = "all"
                else:
                    log.info('payload failed: exeErrorCode=%d' % job['exeErrorCode'])
                    stageout = "log"
            if 'exeErrorDiag' in job['job_report']:
                job['exeErrorDiag'] = job['job_report']['exeErrorDiag']
                if job['exeErrorDiag'] != "":
                    log.warning('payload failed: exeErrorDiag=%s' % job['exeErrorDiag'])

        job['stageout'] = stageout  # output and log file or only log file
        log.debug('adding job to data_out queue (stageout=%s)' % stageout)
        queues.data_out.put(job)


def dump_job_report(job, outputfilename):
    log = logger.getChild(str(job['PandaID']))
    log.debug('in dump_worker_attributes')
    job_report = None
    work_attributes = {'jobStatus': job['state']}
    work_attributes['workdir'] = job['working_dir']
    # # work_attributes['messageLevel'] = logging.getLevelName(log.getEffectiveLevel())
    # work_attributes['timestamp'] = timeStamp()
    # no idea which format is needed for timestamp, guessing
    work_attributes['timestamp'] = time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.gmtime())
    work_attributes['cpuConversionFactor'] = 1.0

    jobreport_path = os.path.join(job['working_dir'], "job_report.json")
    log.debug('parsing %s' % jobreport_path)
    if os.path.exists(jobreport_path):
        # load json
        with open(jobreport_path) as jsonFile:
            job_report = json.load(jsonFile)
    if job_report is not None:
        work_attributes.update(parse_jobreport_data(job_report))
    else:
        log.debug('no job_report object')
    log.info('output worker attributes for Harvester: %s' % work_attributes)

    with open(outputfilename, 'w') as outputfile:
        json.dump(work_attributes, outputfile)
    log.debug('exit dump worker attributes')


def parse_jobreport_data(job_report):
    work_attributes = {}
    if job_report is None or not any(job_report):
        return work_attributes

    # these are default values for job metrics
    core_count = "undef"
    work_attributes["n_events"] = "undef"
    work_attributes["__db_time"] = "undef"
    work_attributes["__db_data"] = "undef"

    class DictQuery(dict):
        def get(self, path, dst_dict, dst_key):
            keys = path.split("/")
            if len(keys) == 0:
                return
            last_key = keys.pop()
            v = self
            for key in keys:
                if key in v and isinstance(v[key], dict):
                    v = v[key]
                else:
                    return
            dst_dict[dst_key] = v[last_key]

    if 'ATHENA_PROC_NUMBER' in os.environ:
        work_attributes['core_count'] = os.environ['ATHENA_PROC_NUMBER']
        core_count = os.environ['ATHENA_PROC_NUMBER']

    dq = DictQuery(job_report)
    dq.get("resource/transform/processedEvents", work_attributes, "n_events")
    dq.get("resource/transform/cpuTimeTotal", work_attributes, "cpuConsumptionTime")
    dq.get("resource/machine/node", work_attributes, "node")
    dq.get("resource/machine/model_name", work_attributes, "cpuConsumptionUnit")
    dq.get("resource/dbTimeTotal", work_attributes, "__db_time")
    dq.get("resource/dbDataTotal", work_attributes, "__db_data")
    dq.get("exitCode", work_attributes, "transExitCode")
    dq.get("exitCode", work_attributes, "exeErrorCode")
    dq.get("exitMsg", work_attributes, "exeErrorDiag")
    dq.get("files/input/subfiles", work_attributes, "nInputFiles")

    if 'resource' in job_report and 'executor' in job_report['resource']:
        j = job_report['resource']['executor']
        exc_report = []
        fin_report = defaultdict(int)
        for v in filter(lambda d: 'memory' in d and ('Max' or 'Avg' in d['memory']), j.itervalues()):
            if 'Avg' in v['memory']:
                exc_report.extend(v['memory']['Avg'].items())
            if 'Max' in v['memory']:
                exc_report.extend(v['memory']['Max'].items())
        for x in exc_report:
            fin_report[x[0]] += x[1]
        work_attributes.update(fin_report)

    if 'files' in job_report and 'input' in job_report['files'] and 'subfiles' in job_report['files']['input']:
                work_attributes['nInputFiles'] = len(job_report['files']['input']['subfiles'])

    workdir_size = get_workdir_size()
    work_attributes['jobMetrics'] = 'core_count=%s n_events=%s db_time=%s db_data=%s workdir_size=%s' % \
                                    (core_count,
                                        work_attributes["n_events"],
                                        work_attributes["__db_time"],
                                        work_attributes["__db_data"],
                                        workdir_size)
    del(work_attributes["__db_time"])
    del(work_attributes["__db_data"])

    return work_attributes


def get_workdir_size():
    c, o, e = pilot.util.container.execute('du -s', shell=True)
    if o is not None:
        return o.split()[0]
    return None


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
