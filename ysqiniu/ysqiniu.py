import qiniu
import logging
import requests
import time


class ListError(Exception):
    def __init__(self):
        super(ListError, self).__init__()


class PFOPError(Exception):
    def __init__(self, status):
        super(PFOPError, self).__init__()
        self.status = status


class YSQiniu(object):
    PFOP_URL = 'http://api.qiniu.com/status/get/prefop?id={}'

    def __init__(self, access_key, secret_key):
        super(YSQiniu, self).__init__()
        self.access_key = access_key
        self.secret_key = secret_key
        self._auth = qiniu.Auth(access_key, secret_key)

    def list(self, bucket_name, prefix=None, limit=None):
        bucket = qiniu.BucketManager(self._auth)
        marker = None
        eof = False
        item = []
        while eof is False:
            ret, eof, info = bucket.list(
                bucket_name, prefix=prefix, marker=marker, limit=limit)
            marker = ret.get('marker', None)
            item += ret['items']
        if eof is not True:
            raise ListError()
        return item

    def list_with_handler(self, bucket_name, handler, prefix=None, limit=None):
        if hasattr(handler, '__call__'):
            logging.info('handler is callable')
            return False
        bucket = qiniu.BucketManager(self._auth)
        marker = None
        eof = False
        while eof is False:
            ret, eof, info = bucket.list(
                bucket_name, prefix=prefix, marker=marker, limit=limit)
            marker = ret.get('marker', None)
            for item in ret['items']:
                handler(item)
        if eof is not True:
            raise ListError()

    def file_exists(self, bucket_name, filename):
        bucket = qiniu.BucketManager(self._auth)
        stat = bucket.stat(bucket_name, filename)
        logging.info(stat)
        if stat[0] is None:
            return False
        if len(stat) > 0 and 'hash' in stat[0]:
            return True
        return False

    def vframe(self, save_bucket_name, saveas, src_bucket_name, src,
               offset=0, width=None, height=None, rotate=None,
               pipeline='vframe', notify_url=None, format='png'):
        pfop = qiniu.PersistentFop(
            auth=self._auth,
            bucket=src_bucket_name,
            pipeline=pipeline,
            notify_url=notify_url
        )
        opargs = {
            'offset': offset
        }
        if width is not None:
            opargs['w'] = width
        if height is not None:
            opargs['h'] = height
        if rotate is not None:
            opargs['rotate'] = rotate
        op = qiniu.build_op('vframe', format,
                            **opargs)
        op = qiniu.op_save(op, save_bucket_name, saveas)
        logging.debug('[op] {}'.format(op))
        ops = [op]
        ret, info = pfop.execute(src, ops, 1)
        return ret, info

    def avconcat(self, save_bucket_name, saveas, base_bucket_name, base,
                 urls, mode='2', format='mp4',
                 pipeline='concatevideo', notify_url=None):
        pfop = qiniu.PersistentFop(
            auth=self._auth,
            bucket=base_bucket_name,
            pipeline=pipeline,
            notify_url=notify_url
        )
        op = qiniu.build_op('avconcat', mode, format=format)
        if len(urls) > 5:
            raise ValueError('cannot append more then 5 videos')
        encoded_keys = [qiniu.urlsafe_base64_encode(url) for url in urls]
        encoded_keys.insert(0, op)
        op = '/'.join(encoded_keys)
        op = qiniu.op_save(op, save_bucket_name, saveas)
        logging.debug('[op] {}'.format(op))
        ops = [op]
        ret, info = pfop.execute(base, ops, 1)
        return ret, info

    def copy_or_avconcat(self, base_bucket_name, base, src_bucket_name, src,
                         urls, mode='2', format='mp4',
                         pipeline='concatevideo', notify_url=None):
        if self.file_exists(base_bucket_name, base):
            # concat
            logging.info('to concat')
            return self.avconcat(
                save_bucket_name=base_bucket_name,
                saveas=base,
                base_bucket_name=base_bucket_name,
                base=base,
                urls=urls
            )
        else:
            # copy
            logging.info('to copy')
            return self.copy(
                save_bucket_name=base_bucket_name,
                saveas=base,
                src_bucket_name=src_bucket_name,
                src=src
            )

    def block_pfop(self, pfop_id, delay=0.4):
        while 1:
            pfop_status = self.pfop_status(pfop_id=pfop_id)
            if pfop_status['code'] == 0:
                break
            if pfop_status['code'] == 3:
                raise PFOPError(pfop_status)
            time.sleep(delay)
        return pfop_status

    def pfop_status(self, pfop_id):
        url = YSQiniu.PFOP_URL.format(pfop_id)
        r = requests.get(url)
        return r.json()

    def copy(self, save_bucket_name, saveas, src_bucket_name, src):
        bucket = qiniu.BucketManager(self._auth)
        return bucket.copy(src_bucket_name, src, save_bucket_name, saveas)

    def move(self, save_bucket_name, saveas, src_bucket_name, src):
        bucket = qiniu.BucketManager(self._auth)
        return bucket.move(src_bucket_name, src, save_bucket_name, saveas)

    def delete(self, bucket_name, src):
        bucket = qiniu.BucketManager(self._auth)
        return bucket.delete(bucket_name, src)

    def private_download_url(self, base_url, expires=3600):
        return self._auth.private_download_url(base_url, expires=expires)
