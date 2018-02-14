#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2016-2017
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

import Queue
import os
import threading
import time
from json import dumps

from pilot.util import https
from pilot.util.config import config
from pilot.util.workernode import get_disk_space, collect_workernode_info, get_node_name
from pilot.util.proxy import get_distinguished_name
from pilot.util.information import get_timefloor

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

    threads = [threading.Thread(target=validate,
                                kwargs={'queues': queues,
                                        'traces': traces,
                                        'args': args}),
               threading.Thread(target=retrieve,
                                kwargs={'queues': queues,
                                        'traces': traces,
                                        'args': args}),
               threading.Thread(target=create_data_payload,
                                kwargs={'queues': queues,
                                        'traces': traces,
                                        'args': args}),
               threading.Thread(target=job_monitor,
                                kwargs={'queues': queues,
                                        'traces': traces,
                                        'args': args})]

    [t.start() for t in threads]


def _validate_job(job):
    """
    (add description)

    :param job:
    :return:
    """
    # valid = random.uniform(0, 100)
    # if valid > 99:
    #     logger.warning('%s: job did not validate correctly -- skipping' % job['PandaID'])
    #     job['errno'] = random.randint(0, 100)
    #     job['errmsg'] = 'job failed random validation'
    #     return False
    return True


def send_state(job, args, state, xml=None):
    """
    Update the server (heartbeat message).

    :param job:
    :param args:
    :param state:
    :param xml:
    :return:
    """

    log = logger.getChild(str(job['PandaID']))
    if state == 'finished':
        log.info('job %d has finished - sending final server update' % job['PandaID'])
    else:
        log.debug('set job state=%s' % state)

    # report the batch system job id, if available
    batchsystem_type, batchsystem_id = get_batchsystem_jobid()

    data = {'jobId': job['PandaID'],
            'state': state,
            'timestamp': time_stamp(),
            'siteName': args.site,
            'node': get_node_name()}

    # error codes
    pilot_error_code = job.get('pilotErrorCode', 0)
    pilot_error_codes = job.get('pilotErrorCodes', [])
    if pilot_error_codes != []:
        log.warning('pilotErrorCodes = %s (will report primary/first error code)' % str(pilot_error_codes))
        data['pilotErrorCode'] = pilot_error_codes[0]
    else:
        data['pilotErrorCode'] = pilot_error_code

    pilot_error_diag = job.get('pilotErrorDiag', 0)
    pilot_error_diags = job.get('pilotErrorDiags', [])
    if pilot_error_diags != []:
        log.warning('pilotErrorDiags = %s (will report primary/first error diag)' % str(pilot_error_diags))
        data['pilotErrorDiag'] = pilot_error_diags[0]
    else:
        data['pilotErrorDiag'] = pilot_error_diag

    data['transExitCode'] = job.get('transExitCode', 0)
    data['exeErrorCode'] = job.get('exeErrorCode', 0)
    data['exeErrorDiag'] = job.get('exeErrorDiag', '')

    data['attemptNr'] = job.get('attemptNr', 0)

    schedulerid = get_job_scheduler_id()
    if schedulerid:
        data['schedulerID'] = schedulerid

    pilotid = get_pilot_id()
    if pilotid:
        use_newmover_tag = 'DEPRECATED'
        version_tag = args.version_tag
        pilot_version = os.environ.get('PILOT_VERSION')

        if batchsystem_type:
            data['pilotID'] = "%s|%s|%s|%s|%s" % (pilotid, use_newmover_tag, batchsystem_type, version_tag, pilot_version)
            data['batchID'] = batchsystem_id,
        else:
            data['pilotID'] = "%s|%s|%s|%s" % (pilotid, use_newmover_tag, version_tag, pilot_version)

    if xml is not None:
        data['xml'] = xml  # urllib.quote_plus(xml)

    try:
        # cmd = args.url + ':' + str(args.port) + 'server/panda/updateJob'
        # if https.request(cmd, data=data) is not None:

        if https.request('{pandaserver}/server/panda/updateJob'.format(pandaserver=config.Pilot.pandaserver),
                         data=data) is not None:

            log.info('server updateJob request completed for job %s' % job['PandaID'])
            return True
    except Exception as e:
        log.warning('while setting job state, Exception caught: %s' % str(e.message))
        pass

    log.warning('set job state=%s failed' % state)
    return False


def validate(queues, traces, args):
    """
    (add description)

    :param queues:
    :param traces:
    :param args:
    :return:
    """

    while not args.graceful_stop.is_set():
        try:
            job = queues.jobs.get(block=True, timeout=1)
        except Queue.Empty:
            continue
        log = logger.getChild(str(job['PandaID']))

        traces.pilot['nr_jobs'] += 1

        # set the environmental variable for the task id
        os.environ['PanDA_TaskID'] = job['taskID']
        logger.info('processing PanDA job %s from task %s' % (job['PandaID'], job['taskID']))

        if _validate_job(job):

            log.debug('creating job working directory')
            job_dir = os.path.join(args.mainworkdir, 'PanDA_Pilot-%s' % job['PandaID'])
            try:
                os.mkdir(job_dir)
                job['working_dir'] = job_dir
            except Exception as e:
                log.debug('cannot create working directory: %s' % str(e))
                queues.failed_jobs.put(job)
                break

            log.debug('symlinking pilot log')
            try:
                os.symlink('../pilotlog.txt', os.path.join(job_dir, 'pilotlog.txt'))
            except Exception as e:
                log.debug('cannot symlink pilot log: %s' % str(e))
#                queues.failed_jobs.put(job)
#                break

            queues.validated_jobs.put(job)
        else:
            log.debug('Failed to validate job=%s' % job['PandaID'])
            queues.failed_jobs.put(job)


def create_data_payload(queues, traces, args):
    """
    (add description)

    :param queues:
    :param traces:
    :param args:
    :return:
    """

    while not args.graceful_stop.is_set():
        try:
            job = queues.validated_jobs.get(block=True, timeout=1)
        except Queue.Empty:
            continue

        queues.data_in.put(job)
        queues.payloads.put(job)


def get_task_id():
    """
    Return the task id for the current job.
    Note: currently the implementation uses an environmental variable to store this number (PanDA_TaskID).

    :return: task id (string). Returns empty string in case of error.
    """

    if "PanDA_TaskID" in os.environ:
        taskid = os.environ["PanDA_TaskID"]
    else:
        logger.warning('PanDA_TaskID not set in environment')
        taskid = ""

    return taskid


def get_dispatcher_dictionary(args):
    """
    Return a dictionary with required fields for the dispatcher getJob operation.

    The dictionary should contain the following fields: siteName, computingElement (queue name),
    prodSourceLabel (e.g. user, test, ptest), diskSpace (available disk space for a job in MB),
    workingGroup, countryGroup, cpu (float), mem (float) and node (worker node name).

    workingGroup, countryGroup and allowOtherCountry
    we add a new pilot setting allowOtherCountry=True to be used in conjunction with countryGroup=us for
    US pilots. With these settings, the Panda server will produce the desired behaviour of dedicated X% of
    the resource exclusively (so long as jobs are available) to countryGroup=us jobs. When allowOtherCountry=false
    this maintains the behavior relied on by current users of the countryGroup mechanism -- to NOT allow
    the resource to be used outside the privileged group under any circumstances.

    :param args: arguments (e.g. containing queue name, queuedata dictionary, etc).
    :returns: dictionary prepared for the dispatcher getJob operation.
    """

    _diskspace = get_disk_space(args.location.queuedata)
    _mem, _cpu = collect_workernode_info()
    _nodename = get_node_name()

    data = {
        'siteName': args.resource,
        'computingElement': args.location.queue,
        'prodSourceLabel': args.job_label,
        'diskSpace': _diskspace,
        'workingGroup': args.working_group,
        'cpu': _cpu,
        'mem': _mem,
        'node': _nodename
    }

    if args.allow_other_country != "":
        data['allowOtherCountry'] = args.allow_other_country

    if args.country_group != "":
        data['countryGroup'] = args.country_group

    if args.job_label == 'self':
        dn = get_distinguished_name()
        data['prodUserID'] = dn

    taskid = get_task_id()
    if taskid != "" and args.allow_same_user:
        data['taskID'] = taskid
        logger.info("will download a new job belonging to task id: %s" % (data['taskID']))

    return data


def retrieve_old(queues, traces, args):

    while not args.graceful_stop.is_set():

        logger.debug('trying to fetch job')

        data = {'siteName': args.location.queue,
                'prodSourceLabel': args.job_label}

        res = https.request('https://pandaserver.cern.ch:25443/server/panda/getJob', data=data)

        if res is None:
            logger.warning('did not get a job -- sleep 1000s and repeat')
            for i in xrange(10000):
                if args.graceful_stop.is_set():
                    break
                time.sleep(0.1)
        else:
            if res['StatusCode'] != 0:
                logger.warning('did not get a job -- sleep 1000s and repeat -- status: %s' % res['StatusCode'])
                for i in xrange(10000):
                    if args.graceful_stop.is_set():
                        break
                    time.sleep(0.1)
            else:
                logger.info('got job: %s -- sleep 1000s before trying to get another job' % res['PandaID'])
                queues.jobs.put(res)
                for i in xrange(10000):
                    if args.graceful_stop.is_set():
                        break
                    time.sleep(0.1)


def retrieve(queues, traces, args):
    """
    Retrieve all jobs from a source.

    The job definition is a json dictionary that is either present in the launch
    directory (preplaced) or downloaded from a server specified by `args.url`.

    The function retrieves the job definition from the proper source and places
    it in the `queues.jobs` queue.

    :param queues: internal queues for job handling.
    :param traces: tuple containing internal pilot states.
    :param args: arguments (e.g. containing queue name, queuedata dictionary, etc).
    """

    # get the job dispatcher dictionary
    data = get_dispatcher_dictionary(args)

    timefloor = 0  # get_timefloor()
    starttime = time.time()

    jobnumber = 0
    getjob_requests = 0
    while not args.graceful_stop.is_set():

        currenttime = time.time()
        if timefloor == 0 and jobnumber > 0:
            logger.warning("since timefloor is set to 0, pilot was only allowed to run one job")
            args.graceful_stop.set()
            break

        if currenttime - starttime > timefloor:
            logger.warning("the pilot has run out of time (timefloor=%d has been passed)" % timefloor)
            args.graceful_stop.set()
            break

        if jobnumber > 0:
            logger.info('since timefloor=%d s and only %d s has passed since launch, pilot can run another job' %
                        (timefloor, currenttime - starttime))

        getjob_requests += 1

        if not proceed_with_getjob(timefloor, starttime, jobnumber, getjob_requests, args.harvester):
            args.graceful_stop.set()
            break

        # get a job definition from a source (file or server)
        res = get_job_definition(args)

        if res is None:
            logger.fatal('fatal error in job download loop - cannot continue')
            break

        if res == {}:
            delay = get_job_retrieval_delay(args.harvester)
            if not args.harvester:
                logger.warning('did not get a job -- sleep %d s and repeat' % delay)
            for i in xrange(delay):
                if args.graceful_stop.is_set():
                    break
                time.sleep(1)
        else:
            if res['StatusCode'] != 0:
                logger.warning('did not get a job -- sleep 60s and repeat -- status: %s' % res['StatusCode'])
                for i in xrange(60):
                    if args.graceful_stop.is_set():
                        break
                    time.sleep(1)
            else:
                logger.info('received job: %s (sleep until the job has finished)' % res['PandaID'])
                queues.jobs.put(res)
                jobnumber += 1
                if args.graceful_stop.is_set():
                    logger.info('graceful stop is currently set')
                else:
                    logger.info('graceful stop is currently not set')
                while not args.graceful_stop.is_set():
                    if job_has_finished(queues):
                        logger.info('graceful stop has been set')
                        break
                    time.sleep(0.5)


def job_has_finished(queues):
    """
    Has the current payload finished?
    :param queues:
    :return: True is the payload has finished
    """

    status = False
    try:
        finishedjob = queues.finished_jobs.get(block=True, timeout=1)
    except Queue.Empty:
        # logger.info("(job still running)")
        pass
    else:
        logger.info("job %d has finished" % finishedjob['PandaID'])
        status = True

    return status
