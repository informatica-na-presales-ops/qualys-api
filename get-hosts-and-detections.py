import apscheduler.schedulers.blocking
import lxml.objectify
import notch
import os
import psycopg2.extras
import requests.auth
import signal
import sys
import time
import urllib.parse

log = notch.make_log('qualys_api.get_hosts_and_detections')


def human_duration(duration: int) -> str:
    if duration > 60:
        minutes = duration // 60
        seconds = duration % 60
        return f'{minutes}m{seconds}s'
    return f'{duration}s'


def upsert_qualys_hosts(cnx, records: list):
    sql = '''
        insert into qualys_hosts (qualys_host_id, cloud_resource_id, synced)
        values (%(qualys_host_id)s, %(cloud_resource_id)s, true)
        on conflict (qualys_host_id) do update set cloud_resource_id = %(cloud_resource_id)s, synced = true
    '''
    with cnx:
        with cnx.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, records)


def upsert_qualys_host_detections(cnx, records: list):
    with cnx:
        with cnx.cursor() as cur:
            sql = '''
                insert into qualys_host_detections (
                    cloud_resource_id, host_id, last_found_at, qid, results, severity,
                    status, synced, type
                ) values (
                    %(cloud_resource_id)s, %(host_id)s, %(last_found_at)s, %(qid)s, %(results)s, %(severity)s,
                    %(status)s, true, %(type)s
                ) on conflict (host_id, qid) do update set
                    cloud_resource_id = %(cloud_resource_id)s, last_found_at = %(last_found_at)s, results = %(results)s,
                    severity = %(severity)s, status = %(status)s, synced = true, type = %(type)s
            '''
            psycopg2.extras.execute_batch(cur, sql, records)


def call(session: requests.Session, url: str, params: dict) -> requests.Response:
    call_start = time.monotonic()
    response = session.get(url, params=params)
    response.raise_for_status()
    call_duration = int(time.monotonic() - call_start)
    log.info(f'Call completed in {human_duration(call_duration)}')
    return response


def get_hosts(session: requests.Session, url: str, cnx):
    function_start = time.monotonic()

    log.info('Getting host list')

    with cnx:
        with cnx.cursor() as cur:
            cur.execute('update qualys_hosts set synced = false where synced is true')

    params = {
        'action': 'list',
        'host_metadata': 'all',
        'tag_set_include': os.getenv('QUALYS_TAG_SET_INCLUDE'),
        'truncation_limit': 500,
        'use_tags': 1,
    }

    call_count = 0
    while True:
        call_count += 1
        log.info(f'{call_count} Calling {url}?{urllib.parse.urlencode(params)}')
        response = call(session, url, params)

        root = lxml.objectify.fromstring(response.content)
        records = []
        for host in root.RESPONSE.HOST_LIST.HOST:
            qualys_host_id = int(host.ID)
            if hasattr(host, 'CLOUD_RESOURCE_ID'):
                log.debug(f'Qualys host id: {qualys_host_id} / Cloud resource id: {host.CLOUD_RESOURCE_ID}')
                records.append({
                    'cloud_resource_id': str(host.CLOUD_RESOURCE_ID),
                    'qualys_host_id': qualys_host_id,
                })
            else:
                log.info(f'Qualys host {qualys_host_id} does not have a cloud resource id')

        if records:
            log.info(f'Pushing {len(records)} host records to database')
            upsert_qualys_hosts(cnx, records)

        if hasattr(root.RESPONSE, 'WARNING'):
            log.info(f'Found a warning: {root.RESPONSE.WARNING.CODE}')
            if root.RESPONSE.WARNING.CODE == 1980:
                p = urllib.parse.urlparse(str(root.RESPONSE.WARNING.URL))
                params = dict(urllib.parse.parse_qsl(p.query))
                continue

        break

    with cnx:
        with cnx.cursor() as cur:
            cur.execute('delete from qualys_hosts where synced is false')

    function_duration = int(time.monotonic() - function_start)
    log.info(f'Getting host list completed in {human_duration(function_duration)}')


def get_detections(session: requests.Session, url: str, cnx):
    function_start = time.monotonic()

    log.info('Getting detection list')

    with cnx:
        with cnx.cursor() as cur:
            cur.execute('update qualys_host_detections set synced = false where synced is true')

    params = {
        'action': 'list',
        'host_metadata': 'all',
        'qids': os.getenv('QUALYS_QIDS'),
        'tag_set_include': os.getenv('QUALYS_TAG_SET_INCLUDE'),
        'truncation_limit': 500,
        'use_tags': 1,
    }

    call_count = 0
    while True:
        call_count += 1
        log.info(f'{call_count} Calling {url}?{urllib.parse.urlencode(params)}')
        response = call(session, url, params)

        root = lxml.objectify.fromstring(response.content)
        records = []
        for host in root.RESPONSE.HOST_LIST.HOST:
            qualys_host_id = int(host.ID)
            if hasattr(host, 'CLOUD_RESOURCE_ID'):
                log.debug(f'Qualys host id: {qualys_host_id} / Cloud resource id: {host.CLOUD_RESOURCE_ID}')
                for detection in host.DETECTION_LIST.DETECTION:
                    qid = int(detection.QID)
                    records.append({
                        'cloud_resource_id': str(host.CLOUD_RESOURCE_ID),
                        'host_id': qualys_host_id,
                        'last_found_at': str(detection.LAST_FOUND_DATETIME),
                        'qid': qid,
                        'results': str(detection.RESULTS),
                        'severity': int(detection.SEVERITY),
                        'status': str(detection.STATUS),
                        'type': str(detection.TYPE),
                    })
            else:
                log.info(f'Qualys host {qualys_host_id} does not have a cloud resource id')

        if records:
            log.info(f'Pushing {len(records)} detection records to database')
            upsert_qualys_host_detections(cnx, records)

        if hasattr(root.RESPONSE, 'WARNING'):
            log.info(f'Found a warning: {root.RESPONSE.WARNING.CODE}')
            if root.RESPONSE.WARNING.CODE == 1980:
                p = urllib.parse.urlparse(str(root.RESPONSE.WARNING.URL))
                params = dict(urllib.parse.parse_qsl(p.query))
                continue

        break

    with cnx:
        with cnx.cursor() as cur:
            cur.execute('delete from qualys_host_detections where synced is false')

    function_duration = int(time.monotonic() - function_start)
    log.info(f'Getting detection list completed in {human_duration(function_duration)}')


def main_job(repeat_interval_hours: int = None):
    main_job_start = time.monotonic()
    log.info('Running the main job')

    cnx = psycopg2.connect(os.getenv('DB'))

    qualys_hostname = os.getenv('QUALYS_HOSTNAME')
    qualys_password = os.getenv('QUALYS_PASSWORD')
    qualys_username = os.getenv('QUALYS_USERNAME')

    s = requests.session()
    s.auth = requests.auth.HTTPBasicAuth(qualys_username, qualys_password)
    s.headers.update({'X-Requested-With': 'Python'})

    url = f'https://{qualys_hostname}/api/2.0/fo/asset/host/'
    get_hosts(s, url, cnx)

    url = f'https://{qualys_hostname}/api/2.0/fo/asset/host/vm/detection/'
    get_detections(s, url, cnx)

    cnx.close()

    if repeat_interval_hours:
        repeat_message = f'see you again in {repeat_interval_hours} hours'
    else:
        repeat_message = 'quitting'

    main_job_duration = int(time.monotonic() - main_job_start)

    log.info(f'Main job completed in {human_duration(main_job_duration)}, {repeat_message}')


def main():
    repeat = os.getenv('REPEAT', 'false').lower() in ('1', 'on', 'true', 'yes')
    if repeat:
        repeat_interval_hours = int(os.getenv('REPEAT_INTERVAL_HOURS', 6))
        log.info(f'This job will repeat every {repeat_interval_hours} hours')
        log.info('Change this value by setting the REPEAT_INTERVAL_HOURS environment variable')
        scheduler = apscheduler.schedulers.blocking.BlockingScheduler()
        scheduler.add_job(main_job, 'interval', args=[repeat_interval_hours], hours=repeat_interval_hours)
        scheduler.add_job(main_job, args=[repeat_interval_hours])
        scheduler.start()
    else:
        main_job()


def handle_sigterm(_signal, _frame):
    sys.exit()


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, handle_sigterm)
    main()
