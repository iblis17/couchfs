import cloudant
import http.client
import logging
import stat

from errno import ENOENT
from fuse import FUSE, FuseOSError, LoggingMixIn, Operations
from json import JSONDecoder
from sys import argv, exit

from utils import is_db, is_doc


class Couch(LoggingMixIn, Operations):
    def __init__(self, uri, user=None, passwd=None):
        assert passwd if user else True, 'Argument "passwd" is required'

        self.credential = (user, passwd) if user else None
        self.uri = uri
        self.account = cloudant.Account(self.uri, auth=self.credential)

    @property
    def _all_dbs(self) -> list:
        return self.account.all_dbs().json()

    def _get_doc(self, path, raw=False) -> dict:
        '''
        :param raw: with the args, we just return the ``response`` object
        :return: the document in json.
                If the doc is unable to decode,
                we return the raw text.
        '''
        res = self.account.get(path)
        code = res.status_code

        if code == http.client.NOT_FOUND:
            raise FuseOSError(ENOENT)
        elif code != http.client.OK:
            self.log.debug('strang status {}'.format(code))
            self.log.debug('content {}{}'.format(
                res.text[:200]),
                ' ...' if len(res.text) > 200 else None
            )

        try:
            doc = res.json()
        except JSONDecoder as e:
            self.log.debug('json decode failed')
            doc = res.text

        return doc if not raw else res

    def read(self, path, size, offset, fh):
        'Returns a string containing the data requested.'
        res = self._get_doc(path[1:], raw=True)
        doc = res.json()

        if not is_doc(doc):
            raise FuseOSError(EIO)

        return res.text.encode()[offset:offset + size]


    def readdir(self, path, fh):
        '''
        Can return either a list of names, or a list of (name, attrs, offset)
        tuples. attrs is a dict as in getattr.
        '''
        ret = ('.', '..')

        if path == '/':
            return ret + tuple(self._all_dbs)

        doc = self._get_doc(path[1:])

        if is_db(doc):
            db = self.account[doc['db_name']]
            docs = tuple(map(
                lambda x: x['id'],
                filter(
                    lambda x: False if x['id'].startswith('_design/') else True,
                    db.all_docs()
                )
            ))
            self.log.debug('All docs of {}: {}'.format(db.uri, docs))
            return ret + docs

    def getattr(self, path, fh=None):
        '''
        Returns a dictionary with keys identical to the stat C structure of
        stat(2).

        st_atime, st_mtime and st_ctime should be floats.

        NOTE: There is an incombatibility between Linux and Mac OS X
        concerning st_nlink of directories. Mac OS X counts all files inside
        the directory, while Linux counts only the subdirectories.
        '''
        if path == '/':
            st_nlink = 2 + len(self._all_dbs)
            return {
                'st_mode': (stat.S_IFDIR | 0o755),
                'st_nlink': st_nlink,
            }

        res = self._get_doc(path[1:], raw=True)
        doc = res.json()

        # stat attrs
        file_type = stat.S_IFREG
        st_nlink = 1
        st_size = 0

        if is_db(doc):
            file_type = stat.S_IFDIR
            st_nlink = 2
        elif is_doc(doc):
            file_type = stat.S_IFREG
            st_nlink = 1
            st_size = len(res.text.encode())

        return {
            'st_mode': (file_type | 0o755),
            'st_nlink': st_nlink,
            'st_size': st_size,
        }


if __name__ == '__main__':
    if len(argv) != 3:
        print('usage: {} <uri> <mountpoint>'.format(argv[0]))
        exit(1)

    logging.basicConfig(level=logging.DEBUG)

    fuse = FUSE(Couch(argv[1]), argv[2], foreground=True, nothreads=True)
