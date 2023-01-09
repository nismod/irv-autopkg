

class StorageBackend(dict):
    """StorageBackend must inherit from dict 
    to enable json serialisation by kombu"""

    datasets_folder_name = "datasets"
    dataset_data_folder_name = "data"

class ProcessingBackend(dict):
    """
    Processing backend for generation and 
        maintenance of interim processing files
    """

    