from cima.goes.storage._goes_data import GoesStorage
from cima.goes.storage._ftp import FTP
from cima.goes.storage._async_ftp import AFTP
from cima.goes.storage._nfs import NFS
from cima.goes.storage._async_nfs import ANFS
from cima.goes.storage._gcs import GCS
from cima.goes.storage._http import HTTP
from cima.goes.storage._file_systems import Storage, StorageInfo, storage_type


def mount_storage(store: StorageInfo) -> Storage:
    if store.stype == storage_type.FTP:
        return FTP(**store.kwargs)
    if store.stype == storage_type.HTTP:
        return HTTP(**store.kwargs)
    if store.stype == storage_type.AFTP:
        return AFTP(**store.kwargs)
    if store.stype == storage_type.GCS:
        return GCS(**store.kwargs)
    if store.stype == storage_type.NFS:
        return NFS()
    if store.stype == storage_type.ANFS:
        return ANFS()
    raise Exception(f'{store.stype.value} not implemented')


def mount_goes_storage(store: StorageInfo) -> GoesStorage:
    if store.stype == storage_type.GCS:
        return GCS(**store.kwargs)
    if store.stype == storage_type.HTTP:
        return HTTP(**store.kwargs)
    raise Exception(f'{store.stype.value} not implemented')

