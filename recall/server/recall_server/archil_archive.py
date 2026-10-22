"""Coordinate evidence existence and deletion with its Archil filesystem."""
from __future__ import annotations

import http.client
import json
import re
from urllib.parse import quote

from .archive import ArchiveError, ArchiveNotFound, OBJECT_KEY_RE, S3_DEADLINE_CONNECT_TIMEOUT


_HOSTS = {
    'aws-us-east-1': 'green.us-east-1.aws.prod.archil.com',
    'aws-us-west-2': 'green.us-west-2.aws.prod.archil.com',
    'aws-eu-west-1': 'green.eu-west-1.aws.prod.archil.com',
    'gcp-us-central1': 'blue.us-central1.gcp.prod.archil.com',
}


class ArchilDisk:
    """Single-attempt requests using the SDK's API-key object authentication."""

    def __init__(self, *, api_key, disk_id, region, backing_bucket, read_timeout):
        if (not isinstance(api_key, str) or not api_key.strip()
                or not isinstance(disk_id, str) or not re.fullmatch(r'dsk-[0-9a-f]{16}', disk_id)
                or region not in _HOSTS):
            raise ValueError('Archil archive configuration is invalid')
        self._key = api_key.strip()
        if not self._key.startswith('key-'):
            self._key = 'key-' + self._key
        self._host = _HOSTS[region]
        self._disk_id = disk_id
        self._read_timeout = read_timeout
        status, _, body = self._request('control.', 'GET', '/api/disks/' + disk_id,
                                        body_limit=1024 * 1024)
        try:
            value = json.loads(body)
            disk = value['data']
            mounts = disk['mounts']
            valid = (status == 200 and value['success'] is True
                     and disk['id'] == disk_id and disk['region'] == region
                     and disk['status'] == 'available' and len(mounts) == 1)
            mount = mounts[0]
            valid = (valid and mount['type'] == 's3' and mount['path'] == '/'
                     and mount['accessMode'] == 'rw' and mount['connectionStatus'] == 'connected'
                     and mount['config']['bucketName'] == backing_bucket
                     and mount['config'].get('bucketPrefix') in (None, '')
                     and mount['config']['bucketEndpoint'] == 'aws')
        except (ValueError, KeyError, TypeError, IndexError, AttributeError):
            valid = False
        if not valid:
            raise ValueError('Archil archive backing bucket does not match')

    def _request(self, service, method, path, *, body_limit=0):
        connection = http.client.HTTPSConnection(
            service + self._host, timeout=S3_DEADLINE_CONNECT_TIMEOUT)
        try:
            connection.connect()
            connection.sock.settimeout(self._read_timeout)
            connection.request(method, path, headers={'Authorization': self._key})
            response = connection.getresponse()
            status = response.status
            length = response.getheader('Content-Length')
            body = response.read(body_limit + 1) if body_limit else b''
            if len(body) > body_limit or not (200 <= status < 300 or status == 404):
                raise ArchiveError('Archil archive request failed')
            return status, length, body
        except ArchiveError:
            raise
        except Exception:
            raise ArchiveError('Archil archive request failed') from None
        finally:
            connection.close()

    def _path(self, key):
        if not isinstance(key, str) or not OBJECT_KEY_RE.fullmatch(key):
            raise ValueError('Archil archive object key is invalid')
        return '/' + self._disk_id + '/' + quote(key, safe='/')

    def size(self, key):
        status, length, _ = self._request('s3.', 'HEAD', self._path(key))
        if status == 404:
            return None
        if status != 200 or not isinstance(length, str) or not re.fullmatch(r'[0-9]+', length):
            raise ArchiveError('Archil archive size is unavailable')
        return int(length)

    def delete(self, key):
        status, _, _ = self._request('s3.', 'DELETE', self._path(key))
        if status not in (200, 204, 404):
            raise ArchiveError('Archil archive deletion was not acknowledged')


class ArchilArchiveClient:
    """Keep original S3 integrity checks and delete through the disk.

    The durable cleanup queue blocks admission while deletion synchronizes.
    Confirm backing absence before acknowledging cleanup; unrelated new
    uploads do not have to wait for Archil's background discovery.
    """

    requires_deletion_claim = True

    def __init__(self, origin, disk, bucket):
        self._origin = origin
        self._disk = disk
        self._bucket = bucket

    def __getattr__(self, name):
        return getattr(self._origin, name)

    def _key(self, kwargs):
        if (set(kwargs) != {'Bucket', 'Key'} or kwargs['Bucket'] != self._bucket
                or not isinstance(kwargs['Key'], str) or not OBJECT_KEY_RE.fullmatch(kwargs['Key'])):
            raise ValueError('Archil archive object scope is invalid')
        return kwargs['Key']

    def head_object(self, **kwargs):
        key = self._key(kwargs)
        try:
            metadata = self._origin.head_object(**kwargs)
        except ArchiveNotFound:
            if self._disk.size(key) is not None:
                raise ArchiveError('Archil archive backing synchronization pending') from None
            raise
        return metadata

    def delete_object(self, **kwargs):
        key = self._key(kwargs)
        if self._disk.size(key) is None:
            raise ArchiveError('Archil archive discovery pending')
        self._disk.delete(key)
        return {}

    def confirm_deleted(self, **kwargs):
        self._key(kwargs)
        try:
            self._origin.head_object(**kwargs)
        except ArchiveNotFound:
            if self._disk.size(kwargs['Key']) is None:
                return
        raise ArchiveError('Archil archive backing synchronization pending')


class HeldArchiveClient:
    """Keep capture available while all admission writers receive the guard."""

    requires_deletion_claim = True

    def __init__(self, origin):
        self._origin = origin

    def __getattr__(self, name):
        return getattr(self._origin, name)

    def delete_object(self, **kwargs):
        raise ArchiveError('evidence archive deletion is held')


def coordinate_evidence_archive(store, values):
    mode = values.get('RECALL_EVIDENCE_DELETE_MODE', 'direct')
    if mode not in {'direct', 'hold', 'archil'}:
        raise ValueError('evidence archive deletion mode is invalid')
    if mode == 'direct':
        return store
    if mode == 'hold':
        store.client = HeldArchiveClient(store.client)
        store.deadline_client = HeldArchiveClient(store.deadline_client)
        return store
    names = ('ARCHIL_API_KEY', 'RECALL_ARCHIL_DISK_ID', 'RECALL_ARCHIL_REGION')
    if (store.compatibility_profile != 'aws-unversioned'
            or any(not isinstance(values.get(name), str) or not values[name].strip() for name in names)):
        raise ValueError('Archil archive configuration is incomplete or versioned')
    disk = ArchilDisk(api_key=values['ARCHIL_API_KEY'], disk_id=values['RECALL_ARCHIL_DISK_ID'],
                      region=values['RECALL_ARCHIL_REGION'], backing_bucket=store.bucket,
                      read_timeout=store.deadline_read_timeout_seconds)
    store.client = ArchilArchiveClient(store.client, disk, store.bucket)
    store.deadline_client = ArchilArchiveClient(store.deadline_client, disk, store.bucket)
    return store
