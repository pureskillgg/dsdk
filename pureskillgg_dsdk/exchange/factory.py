from .dataset import DataexchangeDataset


def create_dataexchange_dataset(dataset_id, /, *, root_path):
    return DataexchangeDataset(dataset_id=dataset_id, root_path=root_path)


def download_dataexchange_dataset_revision(
    output_path, dataset_id, revision_id=None, /
):
    client = create_dataexchange_dataset(dataset_id, root_path=output_path)
    return client.download_revision(revision_id)
