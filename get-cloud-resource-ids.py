import apscheduler.schedulers.blocking
import lxml.objectify
import notch
import os
import psycopg2.extras
import qualysapi
import signal
import sys
import urllib.parse

log = notch.make_log('qualys_api.get_host_details')


def main_job():
    qualys_hostname = os.getenv('QUALYS_HOSTNAME')
    qualys_password = os.getenv('QUALYS_PASSWORD')
    qualys_username = os.getenv('QUALYS_USERNAME')
    q = qualysapi.connect(username=qualys_username, password=qualys_password, hostname=qualys_hostname)

    call_count = 0
    call = '/api/2.0/fo/asset/host/'

    tag_set_include = os.getenv('QUALYS_TAG_SET_INCLUDE')
    params = {'action': 'list', 'host_metadata': 'all', 'use_tags': '1', 'tag_set_include': tag_set_include}

    cnx = psycopg2.connect(os.getenv('DB'))
    with cnx:
        with cnx.cursor() as cur:
            cur.execute('update qualys_hosts set synced = false where synced is true')
            sql = '''
                insert into qualys_hosts (qualys_host_id, cloud_resource_id, synced)
                values (%(qualys_host_id)s, %(cloud_resource_id)s, true)
                on conflict (qualys_host_id) do update set cloud_resource_id = %(cloud_resource_id)s, synced = true
            '''
            while True:
                call_count += 1
                log.info(f'{call_count} Calling {call}?{urllib.parse.urlencode(params)}')
                result = q.request(call, params)
                root = lxml.objectify.fromstring(result.encode())

                records = []
                for host in root.RESPONSE.HOST_LIST.HOST:
                    if hasattr(host, 'CLOUD_RESOURCE_ID'):
                        log.debug(f'Qualys host id: {host.ID} / Cloud resource id: {host.CLOUD_RESOURCE_ID}')
                        records.append({
                            'qualys_host_id': int(host.ID),
                            'cloud_resource_id': str(host.CLOUD_RESOURCE_ID)
                        })
                    else:
                        log.info(f'Qualys host {host.ID} does not have a cloud resource id')
                if records:
                    log.info(f'Pushing {len(records)} records to database')
                    psycopg2.extras.execute_batch(cur, sql, records)

                if hasattr(root.RESPONSE, 'WARNING'):
                    log.info(f'Found a warning: {root.RESPONSE.WARNING.CODE}')
                    if root.RESPONSE.WARNING.CODE == 1980:
                        p = urllib.parse.urlparse(str(root.RESPONSE.WARNING.URL))
                        call = p.path
                        params = dict(urllib.parse.parse_qsl(p.query))
                        continue

                break
            cur.execute('delete from qualys_hosts where synced is false')

    cnx.close()


def main():
    repeat = os.getenv('REPEAT', 'false').lower() in ('1', 'on', 'true', 'yes')
    if repeat:
        repeat_interval_hours = int(os.getenv('REPEAT_INTERVAL_HOURS', 6))
        log.info(f'This job will repeat every {repeat_interval_hours} hours')
        log.info('Change this value by setting the REPEAT_INTERVAL_HOURS environment variable')
        scheduler = apscheduler.schedulers.blocking.BlockingScheduler()
        scheduler.add_job(main_job, 'interval', hours=repeat_interval_hours)
        scheduler.add_job(main_job)
        scheduler.start()
    else:
        main_job()


def handle_sigterm(_signal, _frame):
    sys.exit()


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, handle_sigterm)
    main()
