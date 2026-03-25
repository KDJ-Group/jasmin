import time

import requests
from celery import Celery, Task
from datetime import datetime, timedelta

from .config import *

# @TODO: make configuration loadable from /etc/jasmin/restapi.conf
logger = logging.getLogger('jasmin-restapi')
if len(logger.handlers) == 0:
    logger.setLevel(log_level)
    handler = logging.handlers.TimedRotatingFileHandler(filename=log_file, when=log_rotate)
    handler.setFormatter(logging.Formatter(log_format, log_date_format))
    logger.addHandler(handler)

app = Celery(__name__)
task = app.task
app.config_from_object('jasmin.protocols.rest.config')


class JasminTask(Task):
    def __init__(self):
        Task.__init__(self)

        # Shared namespace
        self.worker_tracker = {'last_req_at': datetime.now(), 'last_req_time': 0, 'throughput': 0}

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.error('Task [%s] failed: %s', task_id, exc)


@task(bind=True, base=JasminTask)
def httpapi_send(self, batch_id, batch_config, message_params, config, message_id=None):
    """Calls Jasmin's /send http api, if we have errback_url and callback_url in batch_config then
    will callback those urls asynchronously to inform user of batch progression.

    :param batch_id: UUID of the batch this message belongs to
    :param batch_config: Batch-level configuration (callback_url, errback_url, etc.)
    :param message_params: Parameters for the /send call (to, content, etc.)
    :param config: Worker-level configuration (throughput, smart_qos)
    :param message_id: Pre-generated tracking message ID assigned at queue time.
        This ID is returned in the batch response and can be used to correlate
        with the actual Jasmin message ID returned by /send.
    """
    try:
        slow_down_seconds = 0
        # Shall we do QoS control ?
        if self.worker_tracker['throughput'] > 0:
            qos_throughput_second = 1 / float(self.worker_tracker['throughput'])
            qos_throughput_ysecond_td = timedelta(microseconds=qos_throughput_second * 1000000)
            qos_delay = datetime.now() - self.worker_tracker['last_req_at']
            if qos_delay < qos_throughput_ysecond_td:
                slow_down_seconds = float((qos_throughput_ysecond_td - qos_delay).microseconds) / 1000000
                logger.debug('QoS: slowing down request by %s/s to meet configured throughput per worker: %s/s',
                             slow_down_seconds, self.worker_tracker['throughput'])

        # Shall we sleep ?
        if slow_down_seconds > 0:
            time.sleep(slow_down_seconds)

        r = requests.get('%s/send' % old_api_uri, params=message_params)
    except requests.exceptions.ConnectionError as e:
        msgid_tag = '[msgid:%s] ' % message_id if message_id else ''
        logger.error('[%s] %sJasmin httpapi connection error: %s' % (batch_id, msgid_tag, e))
        if batch_config.get('errback_url', None):
            batch_callback.delay(batch_config.get('errback_url'), batch_id, message_params['to'], 0,
                                 'HTTPAPI Connection error: %s' % e, message_id)
    except Exception as e:
        msgid_tag = '[msgid:%s] ' % message_id if message_id else ''
        logger.error('[%s] %sUnknown error (%s): %s' % (batch_id, msgid_tag, type(e), e))
        if batch_config.get('errback_url', None):
            batch_callback.delay(batch_config.get('errback_url'), batch_id, message_params['to'], 0,
                                 'Unknown error: %s' % e, message_id)
    else:
        # Useful for QoS control
        self.worker_tracker['last_req_at'] = datetime.now()

        # Smart throughput calculation
        if self.worker_tracker['throughput'] == 0 and config['throughput'] > 0:
            current_throughput = config['throughput']
        else:
            current_throughput = self.worker_tracker['throughput']
        if config['smart_qos'] and self.worker_tracker['last_req_time'] is not None:
            if r.elapsed.total_seconds() > self.worker_tracker['last_req_time']:
                # We have a slower request, we need to slow down the throughput
                if current_throughput > 0 and (current_throughput - (current_throughput * 10 / 100.0)) > 0:
                    logger.debug('Smart QoS: Slowing down throughput %s/s to -10%%', current_throughput)
                    current_throughput = current_throughput - (current_throughput * 10 / 100.0)
                elif current_throughput == 0:
                    logger.debug('Smart QoS: Slowing down throughput %s/s to fixed 0.5/s', current_throughput)
                    current_throughput = 0.5
                    # Else: keep current_throughput as is since it cannot go down to zero
            elif r.elapsed.total_seconds() < self.worker_tracker['last_req_time']:
                # We have a slower request, we need to boost the throughput
                if (current_throughput > 0 and config['throughput'] > 0 and (
                            current_throughput + (current_throughput * 10 / 100.0)) <= config['throughput']):
                    logger.debug('Smart QoS: Boosting throughput %s/s to +10%%', current_throughput)
                    current_throughput = current_throughput + (current_throughput * 10 / 100.0)
                elif current_throughput > 0 and config['throughput'] == 0:
                    logger.debug('Smart QoS: Restoring throughput %s/s to unlimited', current_throughput)
                    current_throughput = 0

        self.worker_tracker['throughput'] = current_throughput
        self.worker_tracker['last_req_time'] = r.elapsed.total_seconds()

        # Return status back
        if r.status_code != 200:
            msgid_tag = '[msgid:%s] ' % message_id if message_id else ''
            logger.error('[%s] %s%s' % (batch_id, msgid_tag, r.text.strip('"')))
            if batch_config.get('errback_url', None):
                batch_callback.delay(
                    batch_config.get('errback_url'), batch_id, message_params['to'], 0,
                    'HTTPAPI error: %s' % r.text.strip('"'), message_id)
        else:
            # Log correlation between tracking messageId and Jasmin's actual message ID.
            # Jasmin /send returns 'Success "jasmin-message-id"' on success.
            # TODO: For full message-level status tracking, store this mapping in Redis
            # (e.g. key: msgid:{message_id} -> value: jasmin_msgid extracted from r.text)
            # and expose it via a GET /secure/batch/{batchId}/messages endpoint.
            if message_id:
                logger.info('[%s] [msgid:%s] Jasmin response: %s' % (batch_id, message_id, r.text.strip('"')))
            if batch_config.get('callback_url', None):
                batch_callback.delay(
                    batch_config.get('callback_url'), batch_id, message_params['to'], 1, r.text, message_id)


@task(bind=True, base=JasminTask)
def batch_callback(self, url, batch_id, to, status, status_text, message_id=None):
    """Calls the configured callback/errback URL to report message status.

    :param url: The callback or errback URL to call
    :param batch_id: UUID of the batch
    :param to: Destination phone number
    :param status: 1 for success, 0 for error
    :param status_text: Descriptive text (Jasmin response or error message)
    :param message_id: Pre-generated tracking message ID (optional, for correlation)
    """
    try:
        if status == 0:
            operation_name = 'Errback'
        else:
            operation_name = 'Callback'

        callback_params = {'batchId': batch_id, 'to': to, 'status': status, 'statusText': status_text}
        # Include messageId in callback if available
        if message_id is not None:
            callback_params['messageId'] = message_id

        requests.get(url, params=callback_params)
    except Exception as e:
        logger.error('(%s) of batch %s to %s failed (%s): %s.' % (operation_name, batch_id, url, type(e), e))
    else:
        logger.info('(%s) of batch %s to %s succeeded.' % (operation_name, batch_id, url))
