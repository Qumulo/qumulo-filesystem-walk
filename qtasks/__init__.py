from multiprocessing.synchronize import Lock
from typing import Generic, Optional, Sequence, Mapping, TypeVar, TYPE_CHECKING
from typing_extensions import Protocol, TypedDict
from qumulo.rest_client import RestClient

if TYPE_CHECKING:
    from multiprocessing.sharedctypes import _Value


class FileInfo(TypedDict):
    dir_id: str
    type: str
    id: str
    file_number: str
    path: str
    name: str
    change_time: str
    creation_time: str
    modification_time: str
    child_count: int
    num_links: int
    datablocks: str
    blocks: str
    metablocks: str
    size: str
    owner: str
    owner_details: Mapping[str, str]
    group: str
    mode: str
    link_target: str


U = TypeVar("U")


class Worker(Protocol[U]):  # pylint: disable=too-few-public-methods
    LOG_FILE_NAME: str
    MAKE_CHANGES: bool

    rc: RestClient
    run_class: U
    result_file_lock: Lock
    # https://github.com/python/typeshed/issues/4266
    action_count: "_Value"
    start_path: str
    snap: Optional[str]


T = TypeVar("T")


class Task(Protocol[T]):
    @staticmethod
    def every_batch(_file_list: Sequence[FileInfo], _work_obj: Worker[T]) -> None:
        ...

    @staticmethod
    def work_start(_work_obj: Worker[T]) -> None:
        ...

    @staticmethod
    def work_done(_work_obj: Worker[T]) -> None:
        ...
