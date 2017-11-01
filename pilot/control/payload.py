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

import Queue
import json
import os
import subprocess
import threading
import time
from collections import defaultdict

from pilot.control.job import send_state
import pilot.util.container

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


def setup_payload(job, out, err):
    """
    (add description)

    :param job:
    :param out:
    :param err:
    :return:
    """
    # log = logger.getChild(str(job['PandaID']))

    # try:
    # create symbolic link for sqlite200 and geomDB in job dir
    #    for db_name in ['sqlite200', 'geomDB']:
    #         src = '/cvmfs/atlas.cern.ch/repo/sw/database/DBRelease/current/%s' % db_name
    #         link_name = 'job-%s/%s' % (job['PandaID'], db_name)
    #         os.symlink(src, link_name)
    # except Exception as e:
    #     log.error('could not create symbolic links to database files: %s' % e)
    #     return False

    return True


def run_payload(job, out, err):
    """
    (add description)

    :param job:
    :param out:
    :param err:
    :return:
    """
    log = logger.getChild(str(job['PandaID']))

    # get the payload command from the user specific code
    # cmd = get_payload_command(job, queuedata)
    athena_version = job['homepackage'].split('/')[1]
    asetup = 'source $ATLAS_LOCAL_ROOT_BASE/user/atlasLocalSetup.sh --quiet; '\
             'source $AtlasSetup/scripts/asetup.sh %s,here; ' % athena_version
    cmd = job['transformation'] + ' ' + job['jobPars']

    log.debug('executable=%s' % asetup + cmd)

    try:
        proc = subprocess.Popen(asetup + cmd,
                                bufsize=-1,
                                stdout=out,
                                stderr=err,
                                cwd=job['working_dir'],
                                shell=True)
    except Exception as e:
        log.error('could not execute: %s' % str(e))
        return None

    log.info('started -- pid=%s executable=%s' % (proc.pid, asetup + cmd))

    return proc


def wait_graceful(args, proc, job):
    """
    (add description)

    :param args:
    :param proc:
    :param job:
    :return:
    """
    log = logger.getChild(str(job['PandaID']))

    breaker = False
    exit_code = None
    while True:
        for i in xrange(100):
            if args.graceful_stop.is_set():
                breaker = True
                log.debug('breaking -- sending SIGTERM pid=%s' % proc.pid)
                proc.terminate()
                break
            time.sleep(0.1)
        if breaker:
            log.debug('breaking -- sleep 3s before sending SIGKILL pid=%s' % proc.pid)
            time.sleep(3)
            proc.kill()
            break

        exit_code = proc.poll()
        log.info('running: pid=%s exit_code=%s' % (proc.pid, exit_code))
        if exit_code is not None:
            break
        else:
            send_state(job, args, 'running')
            continue

    return exit_code


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

            log.debug('opening payload stdout/err logs')
            out = open(os.path.join(job['working_dir'], 'payload.stdout'), 'wb')
            err = open(os.path.join(job['working_dir'], 'payload.stderr'), 'wb')

            log.debug('setting up payload environment')
            send_state(job, args, 'starting')

            exit_code = 1
            if setup_payload(job, out, err):
                log.debug('running payload')
                send_state(job, args, 'running')
                proc = run_payload(job, out, err)
                if proc is not None:
                    exit_code = wait_graceful(args, proc, job)
                    log.info('finished pid=%s exit_code=%s' % (proc.pid, exit_code))

            log.debug('closing payload stdout/err logs')
            out.close()
            err.close()

            if exit_code == 0:
                queues.finished_payloads.put(job)
                job['state'] = 'finished'
            else:
                queues.failed_payloads.put(job)
                job['state'] = 'failed'

            dump_job_report(job, 'job_report_dump.json')

        except Queue.Empty:
            continue


def validate_post(queues, traces, args):
    """
    (add description)

    :param queues:
    :param traces:
    :param args:
    :return:
    """

    while not args.graceful_stop.is_set():
        try:
            job = queues.finished_payloads.get(block=True, timeout=1)
        except Queue.Empty:
            continue
        log = logger.getChild(str(job['PandaID']))

        log.debug('adding job report for stageout')
        with open(os.path.join(job['working_dir'], 'jobReport.json')) as data_file:
            job['job_report'] = json.load(data_file)

        queues.data_out.put(job)


def dump_job_report(self, job, outputfilename):
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

# if __name__ == "__main__":
#    f = open("jobReport.json", "r")
#    jr = json.load(f)
#    f.close()
#    pprint.pprint(parse_jobreport_data(jr))
