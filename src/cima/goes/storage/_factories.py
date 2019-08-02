from cima.goes.storage._goes_data import GoesData
from cima.goes.storage._ftp import FTP
from cima.goes.storage._async_ftp import AFTP
from cima.goes.storage._nfs import NFS
from cima.goes.storage._gcs import GCS
from cima.goes.storage._file_systems import Storage, storage_info, storage_type


def mount_store(store: storage_info) -> Storage:
    if store.stype == storage_type.FTP:
        return FTP(**store.kwargs)
    if store.stype == storage_type.AFTP:
        return AFTP(**store.kwargs)
    if store.stype == storage_type.GCS:
        return GCS(**store.kwargs)
    if store.stype == storage_type.NFS:
        return NFS()
    raise Exception(f'{store.stype.value} not implemented')


def mount_goesdata(goesinfo: storage_info) -> GoesData:
    if goesinfo.stype == storage_type.GCS:
        return GCS(**goesinfo.kwargs)
    raise Exception(f'{goesinfo.stype.value} not implemented')


