import json
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Optional
from google.cloud.storage import Client, Blob, Bucket
from google.cloud.exceptions import NotFound
from cosmas.generated.cosmas_pb2 import FileVersion, PatchList


def get_write_method(logger):
    def write(msg: str):
        if msg == '\n':
            return
        for it in msg.split('\n'):
            logger.info(it)
    return write


def set_log_handler(logger: logging.Logger, file_name: str) -> None:
    if not BucketLoader.DEFAULT_LOG_FOLDER.exists():
        BucketLoader.DEFAULT_LOG_FOLDER.mkdir(parents=True)

    logging.root.setLevel(level=logging.NOTSET)

    file_handler = logging.FileHandler(BucketLoader.DEFAULT_LOG_FOLDER / file_name)
    file_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                                  '%m/%d/%Y %I:%M:%S %p')
    file_handler.setFormatter(formatter)
    logger.handlers = [file_handler]

    logger.write = get_write_method(logger)
    logger.flush = lambda: None


class BucketLoader:
    DEFAULT_LOG_FOLDER = Path('logs')

    def __init__(self,
                 project_name: str,
                 bucket_name: str,
                 download_folder: str):
        self.project_name = project_name
        self.bucket_name = bucket_name
        self.download_folder = Path(download_folder)
        self.logger = logging.getLogger('BucketLoader')

    def _get_bucket(self, client: Client) -> Optional[Bucket]:
        try:
            return client.get_bucket(bucket_or_name=self.bucket_name)
        except NotFound:
            self.logger.error(f'Bucket {self.bucket_name} not found')
            return None
        except Exception as err:
            self.logger.error(err)

    def _get_version_path(self, version: FileVersion) -> Path:
        object_path = self.download_folder / Path('patches', str(version.fileId), str(version.timestamp))
        return object_path

    def _get_content_path(self, version: FileVersion) -> Path:
        object_path = self.download_folder / Path('content', str(version.fileId), str(version.timestamp))
        return object_path

    @staticmethod
    def _parse_version(blob: Blob) -> FileVersion:
        version = FileVersion()
        blob_data = blob.download_as_string()
        version.ParseFromString(blob_data)
        return version

    @staticmethod
    def _store_data(download_path: Path, data: bytes):
        if not download_path.exists():
            if not download_path.parent.exists():
                download_path.parent.mkdir(parents=True)
            download_path.write_bytes(data)

    def _process_versions(self, versions: List[FileVersion]):
        versions.sort(key=lambda v: v.timestamp)
        for version in versions:
            patch_list = PatchList(patches=version.patches)
            self._store_data(self._get_version_path(version), patch_list.SerializeToString())
        self._store_data(self._get_content_path(versions[-1]), versions[-1].content)

    def load(self, continue_from_blob: Optional[str] = None) -> bool:
        current_date = datetime.now().strftime('%Y.%m.%d %H.%M.%S')
        file_name = f'loader {current_date}.log'
        set_log_handler(logger=self.logger, file_name=file_name)

        client = Client(project=self.project_name)
        self.logger.info(f'Created client for project {self.project_name}')

        bucket = self._get_bucket(client)
        if not bucket:
            return False
        self.logger.info(f'Found bucket {self.bucket_name}')

        for last_blob in client.list_blobs(bucket, versions=False):
            last_blob: Blob

            if last_blob.name.endswith('/') or (continue_from_blob and last_blob.name < continue_from_blob):
                continue

            self.logger.info(f'Downloading versions of object {last_blob.name}')
            try:
                last_version = self._parse_version(last_blob)
                object_path = self._get_version_path(last_version)
                if object_path.exists():
                    self.logger.info('No new versions found')
                    continue

                versions = []
                for blob in client.list_blobs(bucket, prefix=last_blob.name, versions=True):
                    self.logger.info(blob.name)
                    file_version = self._parse_version(blob)
                    object_path = self._get_version_path(file_version)
                    self.logger.info(
                        f'fileId={file_version.fileId}, timestamp={file_version.timestamp}, object_path={object_path}')
                    versions.append(file_version)

                new_versions = 0
                versions_to_load = []
                for version in sorted(versions, key=lambda v: -v.timestamp):
                    object_path = self._get_version_path(version)
                    new_versions += not object_path.exists()
                    versions_to_load.append(version)
                    if object_path.exists():
                        break

                """
                We also load the last version among those that are already loaded as it could have been damaged.
                For example, due to an unexpected interrupt of a loader script.
                """
                for version in reversed(versions_to_load):
                    object_path = self._get_version_path(version)
                    patch_list = PatchList(patches=version.patches)
                    self._store_data(object_path, patch_list.SerializeToString())

                if versions_to_load:
                    self._store_data(self._get_content_path(versions_to_load[0]), versions_to_load[0].content)

                self.logger.info(f'{new_versions} new versions of object {last_blob.name} found')

            except Exception as error:
                self.logger.error(f'An unexpected error occurred while downloading objects '
                                  f'from bucket {bucket.name}: {error}')

        return True


def main():
    default_download_folder = str(Path('resources'))

    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, default='config.json')
    parser.add_argument('--continue-from-blob', help='blod id to continue from', type=str, default=None)
    args = parser.parse_args()

    config = json.loads(Path(args.config).read_bytes())

    loader = BucketLoader(
        project_name=config.get('project_name'),
        bucket_name=config.get('bucket_name'),
        download_folder=config.get('download_folder', default_download_folder)
    )
    loader.load(args.continue_from_blob)


if __name__ == '__main__':
    main()
