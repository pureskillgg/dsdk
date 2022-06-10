from .header_tome import create_subheader_tome_from_fs


class HeaderTomeCopierFs:
    def __init__(
        self, *, src_tome_name, tome_collection_root_path, dest_tome_name, ds_type, log
    ):
        self._src_tome_name = src_tome_name
        self._tome_collection_root_path = tome_collection_root_path
        self._dest_tome_name = dest_tome_name
        self._ds_type = ds_type
        self._log = log

    def copy(self):
        create_subheader_tome_from_fs(
            self._dest_tome_name,
            src_tome_name=self._src_tome_name,
            tome_collection_root_path=self._tome_collection_root_path,
            preserve_src_id=True,
            ds_type=self._ds_type,
            is_copied_header=True,
            log=self._log,
        )
