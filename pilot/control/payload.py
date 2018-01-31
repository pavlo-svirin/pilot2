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

from pilot.control.job import send_state

import logging
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
               threading.Thread(target=execute,
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


def execute(queues, traces, args):
    """
    (add description)

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
            else:
                queues.failed_payloads.put(job)

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
        # Harvester only expects the attributes files for certain states.
        # if self.__state in ['finished','failed','running']:
        # with open(self.pilot.args.harvester_workerAttributesFile,'w') as outputfile:
        with open(outputfilename, 'w') as outputfile:
            work_attributes = {'jobStatus': self.state}
            work_attributes['workdir'] = job['working_dir']
            # # work_attributes['messageLevel'] = logging.getLevelName(log.getEffectiveLevel())
            # work_attributes['timestamp'] = timeStamp()
            # no idea which format is needed for timestamp, guessing
            work_attributes['timestamp'] = time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.gmtime())
            work_attributes['cpuConversionFactor'] = 1.0

            core_count = None
            n_events = None
            db_time = None
            db_data = None
            # workdir_size = None

            if 'ATHENA_PROC_NUMBER' in os.environ:
                work_attributes['core_count'] = os.environ['ATHENA_PROC_NUMBER']
                core_count = os.environ['ATHENA_PROC_NUMBER']

            # check if job report json file exists
            job_report = None
            jobreport_path = os.path.join(job['working_dir'], "job_report.json")
            log.debug('parsing %s' % jobreport_path)
            if os.path.exists(jobreport_path):
                # load json
                with open(jobreport_path) as jsonFile:
                    job_report = json.load(jsonFile)
            if job_report is not None:
                if 'resource' in job_report:
                    if 'transform' in job_report['resource']:
                        if 'processedEvents' in job_report['resource']['transform']:
                            work_attributes['n_events'] = job_report['resource']['transform']['processedEvents']
                            n_events = job_report['resource']['transform']['processedEvents']
                        if 'cpuTimeTotal' in job_report['resource']['transform']:
                            work_attributes['cpuConsumptionTime'] = job_report['resource']['transform']['cpuTimeTotal']
                    if 'machine' in job_report['resource']:
                        if 'node' in job_report['resource']['machine']:
                            work_attributes['node'] = job_report['resource']['machine']['node']
                        if 'model_name' in job_report['resource']['machine']:
                            work_attributes['cpuConsumptionUnit'] = job_report['resource']['machine']['model_name']
                    if 'db_timeTotal' in job_report['resource']:
                        db_time = job_report['resource']['db_timeTotal']
                    if 'db_dataTotal' in job_report['resource']:
                        db_data = job_report['resource']['db_dataTotal']

                    if 'executor' in job_report['resource']:
                        if 'memory' in job_report['resource']['executor']:
                            for transform_name, attributes in job_report['resource']['executor'].iteritems():
                                if 'Avg' in attributes['memory']:
                                    for name, value in attributes['memory']['Avg'].iteritems():
                                        if name in work_attributes:
                                            work_attributes[name] += value
                                        else:
                                            work_attributes[name] = value
                                if 'Max' in attributes['memory']:
                                    for name, value in attributes['memory']['Max'].iteritems():
                                        if name in work_attributes:
                                            work_attributes[name] += value
                                        else:
                                            work_attributes[name] = value
                if 'exitCode' in job_report:
                    work_attributes['transExitCode'] = job_report['exitCode']
                    work_attributes['exeErrorCode'] = job_report['exitCode']
                if 'exitMsg' in job_report:
                    work_attributes['exeErrorDiag'] = job_report['exitMsg']
                if 'files' in job_report:
                    if 'input' in job_report['files']:
                        if 'subfiles' in job_report['files']['input']:
                            work_attributes['nInputFiles'] = len(job_report['files']['input']['subfiles'])

                if core_count and n_events and db_time and db_data:
                    c, o, e = self.call('du -s', shell=True)
                    work_attributes['jobMetrics'] = 'core_count=%s n_events=%s db_time=%s db_data=%s workdir_size=%s' % (core_count,
                                                                                                                         n_events,
                                                                                                                         db_time,
                                                                                                                         db_data,
                                                                                                                         o.split()[0])

            else:
                log.debug('no job_report object')
            log.info('output worker attributes for Harvester: %s' % work_attributes)
            json.dump(work_attributes, outputfile)
        # else:
        #    log.debug(' %s is not a good state' % self.state)
        log.debug('exit dump worker attributes')
